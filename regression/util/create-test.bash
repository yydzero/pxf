#!/usr/bin/env bash

test_name=${1:?Must provide name}
group_list=${2:?Must provide groups, comma-delimited}

read -ra groups <<< "$(echo "$group_list" | tr , ' ')"
echo "Creating $test_name in the following groups: ${groups[*]}"

for g in "${groups[@]}"; do
	grep "_FDW_${test_name}" "schedules/fdw_${g}_schedule">/dev/null || echo "test: _FDW_$test_name" >> "schedules/fdw_${g}_schedule"
	grep "_${test_name}" "schedules/${g}_schedule">/dev/null || echo "test: _$test_name" >> "schedules/${g}_schedule"
done

tinc_test_name=$(find ~/workspace/pxf -name "${test_name}.java")

echo "$tinc_test_name"
snake_case_test_name=$(gsed <<< "${test_name}" -Ee 's/(^[A-Z])/\L\1/' | gsed -Ee 's/([A-Z]+)/_\L\1/g')

mapfile -t classes < <(grep runTincTest "${tinc_test_name}" | sed -e 's/.*runTincTest("//' -e 's/\.runTest");//')
for class in "${classes[@]}"; do
	echo "-- sql file for ${class}:"
	cat "../automation/tincrepo/main/${class//./\/}/sql/"*.sql
done > /tmp/query-files

cat <<-EOF | tee "sql/${test_name}.sql" > "expected/${test_name}.out"
	-- ${tinc_test_name}
	\!%HDFS_CMD% dfs -rm -r -f /tmp/pxf_automation_data/${test_name}/

	-- External Table test
	CREATE EXTERNAL TABLE ${snake_case_test_name}_external_table
		(name TEXT, num INTEGER, dub DOUBLE PRECISION, longNum BIGINT, bool BOOLEAN)
		LOCATION('pxf://tmp/pxf_automation_data/${test_name}/data.csv?PROFILE=hdfs:csv')
		FORMAT 'CSV' (DELIMITER ',');

	$(< /tmp/query-files)

	-- clean up HDFS
	%CLEAN_UP%\!%HDFS_CMD% dfs -rm -r /tmp/pxf_automation_data/${test_name}
EOF

cat <<-EOF | tee "sql/FDW_${test_name}.sql" > "expected/FDW_${test_name}.out"
	-- ${tinc_test_name}
	\!%HDFS_CMD% dfs -rm -r -f /tmp/pxf_automation_data/${test_name}/

	-- FDW test
	CREATE SERVER ${snake_case_test_name}_server
		FOREIGN DATA WRAPPER hdfs_pxf_fdw
		OPTIONS (config 'default');
	CREATE USER MAPPING FOR CURRENT_USER SERVER ${snake_case_test_name}_server;
	CREATE FOREIGN TABLE ${snake_case_test_name}_foreign_table (
			name TEXT,
			num INTEGER,
			dub DOUBLE PRECISION,
			longNum BIGINT,
			bool BOOLEAN
		) SERVER ${snake_case_test_name}_server
		OPTIONS (resource '/tmp/pxf_automation_data/${test_name}/data.csv', format 'csv');
	$(< /tmp/query-files)

	-- clean up HDFS
	%CLEAN_UP%\!%HDFS_CMD% dfs -rm -r /tmp/pxf_automation_data/${test_name}
EOF
