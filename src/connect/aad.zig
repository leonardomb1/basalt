//! Azure AD token acquisition for the SQL Server / Dataverse TDS endpoints.
//! basalt's TDS driver does SQL logins; the Dataverse and Azure SQL endpoints
//! want an Azure AD access token instead. Given an AAD username + password this
//! gets one with NO app registration, mirroring what Microsoft.Data.SqlClient's
//! "Active Directory Password" does:
//!
//!   - Managed (cloud) accounts -> OAuth2 ROPC against login.microsoftonline.com.
//!   - Federated (ADFS) accounts -> realm discovery, then a WS-Trust 1.3
//!     username/password request to the on-prem STS for a SAML assertion, which
//!     is exchanged at AAD via the SAML-bearer grant. (No MFA: the password flow
//!     can't satisfy an interactive MFA challenge.)
//!
//! The well-known ADO.NET first-party client id is pre-consented in every tenant,
//! so no registration is required.

const std = @import("std");
const httpx = @import("http.zig");

/// Microsoft.Data.SqlClient's built-in client id for AAD auth (first-party,
/// pre-consented) and the Azure SQL resource the Dataverse TDS endpoint accepts.
pub const ado_client_id = "2fd908ad-0664-4344-b9be-cd3e8b574c38";
pub const sql_resource = "https://database.windows.net";

/// Token for an AAD username/password, auto-detecting managed vs federated.
/// `resource` is the audience (no trailing slash), e.g. https://database.windows.net.
pub fn passwordToken(
    gpa: std.mem.Allocator,
    client_id: []const u8,
    username: []const u8,
    password: []const u8,
    resource: []const u8,
) ![]const u8 {
    const realm = getUserRealm(gpa, username) catch Realm{ .federated = false, .sts_url = "" };
    defer if (realm.sts_url.len > 0) gpa.free(realm.sts_url);
    const tenant = tenantOf(username);
    if (realm.federated and realm.sts_url.len > 0) {
        const assertion = try wsTrustAssertion(gpa, username, password, realm.sts_url);
        defer gpa.free(assertion);
        return samlBearerToken(gpa, tenant, client_id, assertion, resource);
    }
    return ropcToken(gpa, tenant, client_id, username, password, resource);
}

fn tenantOf(upn: []const u8) []const u8 {
    const at = std.mem.indexOfScalar(u8, upn, '@') orelse return "organizations";
    return upn[at + 1 ..];
}

// --- realm discovery -------------------------------------------------------

const Realm = struct { federated: bool, sts_url: []const u8 };

/// GET getuserrealm.srf -> {NameSpaceType, AuthURL}. For a federated domain,
/// derives the WS-Trust 1.3 usernamemixed endpoint from the AuthURL host.
fn getUserRealm(gpa: std.mem.Allocator, upn: []const u8) !Realm {
    var client = httpx.initClient(gpa);
    defer client.deinit();
    var login_enc = std.array_list.Managed(u8).init(gpa);
    defer login_enc.deinit();
    try formEncode(&login_enc, upn);
    const url = try std.fmt.allocPrint(gpa, "https://login.microsoftonline.com/getuserrealm.srf?login={s}&api-version=1.0", .{login_enc.items});
    defer gpa.free(url);
    var aw = std.Io.Writer.Allocating.init(gpa);
    defer aw.deinit();
    _ = try client.fetch(.{ .method = .GET, .location = .{ .url = url }, .response_writer = &aw.writer });
    const root = std.json.parseFromSliceLeaky(std.json.Value, gpa, aw.writer.buffered(), .{}) catch return Realm{ .federated = false, .sts_url = "" };
    const obj = switch (root) {
        .object => |o| o,
        else => return Realm{ .federated = false, .sts_url = "" },
    };
    const ns = if (obj.get("NameSpaceType")) |v| (if (v == .string) v.string else "") else "";
    if (!std.ascii.eqlIgnoreCase(ns, "Federated")) return Realm{ .federated = false, .sts_url = "" };
    // AuthURL = https://fs.host/adfs/ls/?... -> https://fs.host/adfs/services/trust/13/usernamemixed
    const auth = if (obj.get("AuthURL")) |v| (if (v == .string) v.string else "") else "";
    const host = hostOf(auth) orelse return Realm{ .federated = true, .sts_url = "" };
    const sts = try std.fmt.allocPrint(gpa, "https://{s}/adfs/services/trust/13/usernamemixed", .{host});
    return Realm{ .federated = true, .sts_url = sts };
}

fn hostOf(url: []const u8) ?[]const u8 {
    const s = if (std.mem.indexOf(u8, url, "://")) |i| url[i + 3 ..] else url;
    const end = std.mem.indexOfAny(u8, s, "/:?") orelse s.len;
    return if (end == 0) null else s[0..end];
}

// --- WS-Trust (ADFS) -------------------------------------------------------

/// POST a WS-Trust 1.3 RST with the username/password to the ADFS usernamemixed
/// endpoint and return the SAML assertion XML (caller owns it).
fn wsTrustAssertion(gpa: std.mem.Allocator, username: []const u8, password: []const u8, sts_url: []const u8) ![]const u8 {
    const now = std.time.timestamp();
    const created = try iso8601(gpa, now);
    defer gpa.free(created);
    const expires = try iso8601(gpa, now + 600);
    defer gpa.free(expires);
    const mid = try uuidHex(gpa);
    defer gpa.free(mid);
    const uid = try uuidHex(gpa);
    defer gpa.free(uid);
    const u_esc = try xmlEscape(gpa, username);
    defer gpa.free(u_esc);
    const p_esc = try xmlEscape(gpa, password);
    defer gpa.free(p_esc);

    const envelope = try std.fmt.allocPrint(gpa,
        \\<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"><s:Header><a:Action s:mustUnderstand="1">http://docs.oasis-open.org/ws-sx/ws-trust/200512/RST/Issue</a:Action><a:MessageID>urn:uuid:{s}</a:MessageID><a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo><a:To s:mustUnderstand="1">{s}</a:To><o:Security s:mustUnderstand="1" xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"><u:Timestamp u:Id="_0"><u:Created>{s}</u:Created><u:Expires>{s}</u:Expires></u:Timestamp><o:UsernameToken u:Id="uuid-{s}"><o:Username>{s}</o:Username><o:Password>{s}</o:Password></o:UsernameToken></o:Security></s:Header><s:Body><trust:RequestSecurityToken xmlns:trust="http://docs.oasis-open.org/ws-sx/ws-trust/200512"><wsp:AppliesTo xmlns:wsp="http://schemas.xmlsoap.org/ws/2004/09/policy"><a:EndpointReference><a:Address>urn:federation:MicrosoftOnline</a:Address></a:EndpointReference></wsp:AppliesTo><trust:KeyType>http://docs.oasis-open.org/ws-sx/ws-trust/200512/Bearer</trust:KeyType><trust:RequestType>http://docs.oasis-open.org/ws-sx/ws-trust/200512/Issue</trust:RequestType></trust:RequestSecurityToken></s:Body></s:Envelope>
    , .{ mid, sts_url, created, expires, uid, u_esc, p_esc });
    defer gpa.free(envelope);

    var client = httpx.initClient(gpa);
    defer client.deinit();
    var aw = std.Io.Writer.Allocating.init(gpa);
    defer aw.deinit();
    const res = client.fetch(.{
        .method = .POST,
        .location = .{ .url = sts_url },
        .headers = .{ .content_type = .{ .override = "application/soap+xml; charset=utf-8" } },
        .payload = envelope,
        .response_writer = &aw.writer,
    }) catch |e| {
        std.debug.print("ADFS WS-Trust request failed ({s}): {s}\n", .{ @errorName(e), sts_url });
        return error.AadTokenFailed;
    };
    const body = aw.writer.buffered();
    const assertion = extractElement(body, "Assertion") orelse {
        // Surface the SOAP fault reason (<s:Text>) — distinguishes a bad password
        // from an envelope/format problem.
        const reason = extractElement(body, "Text") orelse extractElement(body, "faultstring") orelse "";
        std.debug.print("ADFS WS-Trust http {d}; fault: {s}\n", .{ @intFromEnum(res.status), reason });
        return error.AadTokenFailed;
    };
    return gpa.dupe(u8, assertion);
}

/// Extract `<[ns:]Name ...>...</[ns:]Name>` (the first occurrence), namespaces
/// included. ADFS assertions are self-contained, so the slice stands alone.
fn extractElement(xml: []const u8, name: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, xml, i, name)) |p| {
        // require an element start just before: '<' or '<prefix:'
        var s = p;
        while (s > 0 and (std.ascii.isAlphanumeric(xml[s - 1]) or xml[s - 1] == ':' or xml[s - 1] == '_')) s -= 1;
        if (s == 0 or xml[s - 1] != '<') {
            i = p + name.len;
            continue;
        }
        const open_lt = s - 1;
        const prefix = xml[s..p]; // "" or "ns:"
        // build the matching close tag "</prefix:Name>"
        var close_buf: [64]u8 = undefined;
        const close = std.fmt.bufPrint(&close_buf, "</{s}{s}>", .{ prefix, name }) catch return null;
        const close_at = std.mem.indexOfPos(u8, xml, p, close) orelse return null;
        return xml[open_lt .. close_at + close.len];
    }
    return null;
}

// --- token exchanges -------------------------------------------------------

/// SAML-bearer grant: exchange the assertion for an AAD access token (v1.0
/// endpoint with `resource`, like ADAL's federated flow).
fn samlBearerToken(gpa: std.mem.Allocator, tenant: []const u8, client_id: []const u8, assertion: []const u8, resource: []const u8) ![]const u8 {
    const enc = std.base64.standard.Encoder;
    const b64 = try gpa.alloc(u8, enc.calcSize(assertion.len));
    defer gpa.free(b64);
    _ = enc.encode(b64, assertion);

    var body = std.array_list.Managed(u8).init(gpa);
    defer body.deinit();
    try body.appendSlice("grant_type=");
    try formEncode(&body, "urn:ietf:params:oauth:grant-type:saml1_1-bearer");
    try appendForm(&body, "assertion", b64);
    try appendForm(&body, "client_id", client_id);
    try appendForm(&body, "resource", resource);

    const url = try std.fmt.allocPrint(gpa, "https://login.microsoftonline.com/{s}/oauth2/token", .{tenant});
    defer gpa.free(url);
    return postForToken(gpa, url, body.items);
}

/// OAuth2 ROPC (managed cloud accounts) against the v2.0 endpoint.
pub fn ropcToken(
    gpa: std.mem.Allocator,
    tenant: []const u8,
    client_id: []const u8,
    username: []const u8,
    password: []const u8,
    resource: []const u8,
) ![]const u8 {
    var body = std.array_list.Managed(u8).init(gpa);
    defer body.deinit();
    try body.appendSlice("grant_type=password");
    try appendForm(&body, "client_id", client_id);
    try appendForm(&body, "username", username);
    try appendForm(&body, "password", password);
    const scope = try std.fmt.allocPrint(gpa, "{s}/.default", .{resource});
    defer gpa.free(scope);
    try appendForm(&body, "scope", scope);

    const url = try std.fmt.allocPrint(gpa, "https://login.microsoftonline.com/{s}/oauth2/v2.0/token", .{tenant});
    defer gpa.free(url);
    return postForToken(gpa, url, body.items);
}

fn postForToken(gpa: std.mem.Allocator, url: []const u8, body: []const u8) ![]const u8 {
    var client = httpx.initClient(gpa);
    defer client.deinit();
    var aw = std.Io.Writer.Allocating.init(gpa);
    defer aw.deinit();
    const res = client.fetch(.{
        .method = .POST,
        .location = .{ .url = url },
        .headers = .{ .content_type = .{ .override = "application/x-www-form-urlencoded" } },
        .payload = body,
        .response_writer = &aw.writer,
    }) catch |e| {
        std.debug.print("aad token request failed ({s})\n", .{@errorName(e)});
        return error.AadTokenFailed;
    };
    const resp = aw.writer.buffered();
    if (@intFromEnum(res.status) != 200) {
        std.debug.print("aad token http {d}: {s}\n", .{ @intFromEnum(res.status), resp[0..@min(resp.len, 600)] });
        return error.AadTokenFailed;
    }
    const root = std.json.parseFromSliceLeaky(std.json.Value, gpa, resp, .{}) catch return error.AadTokenFailed;
    const tv = switch (root) {
        .object => |o| o.get("access_token") orelse return error.AadTokenFailed,
        else => return error.AadTokenFailed,
    };
    return switch (tv) {
        .string => |s| try gpa.dupe(u8, s),
        else => error.AadTokenFailed,
    };
}

// --- small helpers ---------------------------------------------------------

fn appendForm(buf: *std.array_list.Managed(u8), key: []const u8, val: []const u8) !void {
    try buf.append('&');
    try buf.appendSlice(key);
    try buf.append('=');
    try formEncode(buf, val);
}

fn formEncode(buf: *std.array_list.Managed(u8), val: []const u8) !void {
    const hex = "0123456789ABCDEF";
    for (val) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '-' or c == '_' or c == '.' or c == '~') {
            try buf.append(c);
        } else {
            try buf.append('%');
            try buf.append(hex[c >> 4]);
            try buf.append(hex[c & 0xF]);
        }
    }
}

fn xmlEscape(gpa: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(gpa);
    for (s) |c| switch (c) {
        '&' => try out.appendSlice("&amp;"),
        '<' => try out.appendSlice("&lt;"),
        '>' => try out.appendSlice("&gt;"),
        '"' => try out.appendSlice("&quot;"),
        '\'' => try out.appendSlice("&apos;"),
        else => try out.append(c),
    };
    return out.toOwnedSlice();
}

fn iso8601(gpa: std.mem.Allocator, secs: i64) ![]const u8 {
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(secs) };
    const ed = es.getEpochDay();
    const yd = ed.calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.allocPrint(gpa, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.000Z", .{
        yd.year,                  md.month.numeric(),        md.day_index + 1,
        ds.getHoursIntoDay(),     ds.getMinutesIntoHour(),   ds.getSecondsIntoMinute(),
    });
}

fn uuidHex(gpa: std.mem.Allocator) ![]const u8 {
    var b: [16]u8 = undefined;
    std.crypto.random.bytes(&b);
    return std.fmt.allocPrint(gpa, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    });
}

test "extractElement pulls a namespaced assertion" {
    const xml =
        "<t:RST><t:RequestedSecurityToken>" ++
        "<saml:Assertion xmlns:saml=\"urn\" Id=\"_x\">BODY<saml:Conditions/></saml:Assertion>" ++
        "</t:RequestedSecurityToken></t:RST>";
    const a = extractElement(xml, "Assertion").?;
    try std.testing.expect(std.mem.startsWith(u8, a, "<saml:Assertion"));
    try std.testing.expect(std.mem.endsWith(u8, a, "</saml:Assertion>"));
    try std.testing.expect(std.mem.indexOf(u8, a, "BODY") != null);
}
