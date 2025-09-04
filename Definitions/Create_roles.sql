create role if not exists ROLE_SUPPLY_ADMIN;     -- full administrative control
create role if not exists ROLE_SUPPLY_DATAENG;   -- build & operate data/ML pipelines
create role if not exists ROLE_SUPPLY_PLANNER;   -- business users (planner) read/app
create role if not exists ROLE_SUPPLY_VIEWER;    -- read-only app/API access

-- Grant hierarchy (adjust to your org standards)
grant role ROLE_SUPPLY_VIEWER  to role ROLE_SUPPLY_PLANNER;
grant role ROLE_SUPPLY_PLANNER to role ROLE_SUPPLY_DATAENG;
grant role ROLE_SUPPLY_DATAENG to role ROLE_SUPPLY_ADMIN;