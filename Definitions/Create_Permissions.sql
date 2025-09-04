grant usage on database SUPPLY_COPILOT to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER, ROLE_SUPPLY_DATAENG, ROLE_SUPPLY_ADMIN;

-- Schema usage
grant usage on all schemas in database SUPPLY_COPILOT to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER, ROLE_SUPPLY_DATAENG, ROLE_SUPPLY_ADMIN;
grant usage on future schemas in database SUPPLY_COPILOT to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER, ROLE_SUPPLY_DATAENG, ROLE_SUPPLY_ADMIN;

-- Object-level defaults (tighten/expand per need)
-- APP: read-only for viewers/planners; write for data engineers/admins (if needed)
grant select on all tables in schema SUPPLY_APP to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;
grant select on future tables in schema SUPPLY_APP to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;
grant select on all views in schema SUPPLY_APP to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;
grant select on future views in schema SUPPLY_APP to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;

-- SILVER/GOLD/FEATURES/ML/META: typical devops split
-- Read for planners/viewers; write for data engineers/admins
-- (Repeat for each schema as appropriate)
grant select on all tables in schema SUPPLY_SILVER to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;
grant select on future tables in schema SUPPLY_SILVER to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;
grant select on all views in schema SUPPLY_SILVER to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;
grant select on future views in schema SUPPLY_SILVER to role ROLE_SUPPLY_VIEWER, ROLE_SUPPLY_PLANNER;

grant all privileges on all schemas in database SUPPLY_COPILOT to role ROLE_SUPPLY_DATAENG;
grant all privileges on future schemas in database SUPPLY_COPILOT to role ROLE_SUPPLY_DATAENG;

-- Admin full control
grant all privileges on database SUPPLY_COPILOT to role ROLE_SUPPLY_ADMIN;