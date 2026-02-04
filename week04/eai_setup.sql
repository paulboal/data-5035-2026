USE SCHEMA DATA5035.SPRING26;

ALTER NETWORK RULE open_meteo_rule
  SET VALUE_LIST = ('open-meteo.com','archive-api.open-meteo.com');

CREATE EXTERNAL ACCESS INTEGRATION open_meteo_integration
  ALLOWED_NETWORK_RULES = (open_meteo_rule)
  ENABLED = true;
  
GRANT USAGE ON NETWORK RULE open_meteo_rule TO ROLE snowflake_learning_role;

GRANT USAGE ON INTEGRATION open_meteo_integration TO ROLE snowflake_learning_role;



CREATE NETWORK RULE washu_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('washu.edu');

ALTER NETWORK RULE open_meteo_rule
  SET VALUE_LIST = ('washu.edu','happenings.washu.edu');


CREATE EXTERNAL ACCESS INTEGRATION washu_integration
  ALLOWED_NETWORK_RULES = (washu_rule)
  ENABLED = true;
  
GRANT USAGE ON NETWORK RULE washu_rule TO ROLE snowflake_learning_role;

GRANT USAGE ON INTEGRATION washu_integration TO ROLE snowflake_learning_role;