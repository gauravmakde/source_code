CREATE OR REPLACE TEMPORARY VIEW json_test USING json
OPTIONS(path "s3://acedev-etl/upas/gitlab_projects_raw/", partition "(year, month, day)");

insert overwrite table gitlab_project_info
select
app_id
, team_id
, archived
, fullPath
, httpUrlToRepo
from json_test
  where
  year = year(current_date()-1)
  and month = month(current_date()-1)
  and day = day(current_date()-1);
