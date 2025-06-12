@echo off
echo Testing database connections...
echo.

echo Testing Trajectory Database:
docker exec mock_trajectory_db psql -U postgres -d trajectory -c "SELECT version();"
echo.

echo Testing Business Database:
docker exec mock_business_db psql -U postgres -d business -c "SELECT version();"
echo.

echo Testing Map Database:
docker exec mock_map_db psql -U postgres -d mapdb -c "SELECT version();"
echo.

echo Database tests completed.
pause 