@batch

connection itsm = http
  base_url = "https://itsm-kaefer.saasiteu.com"
  auth = "login_json"
  login_path = "/api/rest/authentication/login"
  body_tenant = "itsm-kaefer.saasiteu.com"
  body_username = env("ITSM_USER")
  body_password = secret("ITSM_PASS")
  body_role = "Kaefer-ITBasicUser"

read itsm "/api/odata/businessobject/incidents?$filter=CustomerLocation_ITSM eq 'Brazil' and CreatedDateTime ge 2026-06-11" @[items = "value", paginate = offset, page_param = "$skip", size_param = "$top", page_size = 100, stop_short]
  | write csv "text.csv"
