import subprocess
import datetime
import argparse

BEELINE_CMD_TEMPLATE = "beeline -u '<jdbc_connection_string>' -e \"{}\""


def execute_beeline(query):
    command = BEELINE_CMD_TEMPLATE.format(query)
    try:
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        print("Error executing beeline: {}".format(e))


def get_table_location(table_name):
    query = "DESCRIBE FORMATTED `{}`".format(table_name)
    command = BEELINE_CMD_TEMPLATE.format(query)
    output = subprocess.check_output(command, shell=True)
    for line in output.splitlines():
        if line.strip().startswith("Location"):
            return line.split(":", 1)[-1].strip()
    return None


def get_table_partitions(table_name):
    query = "SHOW PARTITIONS `{}`".format(table_name)
    command = BEELINE_CMD_TEMPLATE.format(query)
    output = subprocess.check_output(command, shell=True)
    return output.splitlines()


def archive_partition(table_name, partition, base_archive_path):
    # Create directory for archived partition using HDFS commands
    archive_dir = "{}/{}".format(base_archive_path, table_name)
    subprocess.call(["hdfs", "dfs", "-mkdir", "-p", archive_dir])

    # Archive partition to base archive path using HDFS commands
    partition_path = get_table_location(table_name) + "/" + partition.replace("/", "")
    subprocess.call(["hdfs", "dfs", "-cp", partition_path, archive_dir])

    # Remove archived partition from the table
    drop_query = "ALTER TABLE `{}` DROP PARTITION ({})".format(table_name, partition)
    execute_beeline(drop_query)


def main(file_path, base_archive_path):
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    min_year = datetime.datetime.now().year - 11

    with open(file_path, 'r') as f:
        for line in f:
            table_name = line.strip()
            if not table_name:
                continue

            # Step 2: Get table location
            table_location = get_table_location(table_name)

            # Step 3: Insert operation into archival_operation table
            insert_query = "INSERT INTO TABLE archival_operation VALUES ('{}', '{}', '{}', '{}')".format(table_name, table_location, "operation_id_placeholder", current_date)
            execute_beeline(insert_query)

            # Step 4-6: Get partitions and identify partitions older than 11 years
            partitions = get_table_partitions(table_name)
            for partition in partitions:
                partition_values = partition.split('/')
                partition_year_str = None
                for value in partition_values:
                    if "=" in value:
                        key, val = value.split("=")
                        if len(val) == 4 and val.isdigit():
                            partition_year_str = val
                            break

                if partition_year_str and int(partition_year_str) <= min_year:
                    # Step 7: Insert into table_partitions_archival
                    insert_partition_query = "INSERT INTO TABLE table_partitions_archival VALUES ('{}', '{}')".format(table_name, partition)
                    execute_beeline(insert_partition_query)

                    # Step 8-9: Archive partition and delete it
                    archive_partition(table_name, partition, base_archive_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Archive old Hadoop table partitions.')
    parser.add_argument('file_path', help='Path to the file containing Hadoop table names')
    parser.add_argument('base_archive_path', help='Base HDFS path for archiving partitions')
    args = parser.parse_args()

    main(args.file_path, args.base_archive_path)
