import os
import datetime
import subprocess
import shutil

# Define base archive path
ARCHIVE_BASE_PATH = '/path/to/archive'

# Define the retention period (in years)
RETENTION_YEARS = 11

# Read table list from file
def read_table_list(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

# Check the age of the partition using beeline
# Assuming partition name is in 'YYYY-MM-DD' format
def is_partition_older_than_beeline(table_name, partition_name, years):
    # Run beeline command to get partition information
    show_partition_command = (
        "beeline -u 'jdbc:hive2://your_hive_server' "
        "-e \"SHOW PARTITIONS {}\""
        .format(table_name)
    )
    output = subprocess.check_output(show_partition_command, shell=True)
    partitions = output.splitlines()

    # Find partition and check its age
    for partition in partitions:
        if partition_name in partition:
            try:
                partition_date = datetime.datetime.strptime(partition_name, '%Y-%m-%d')
            except ValueError:
                # If partition_name does not match the expected format, skip it
                return False

            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=years * 365)
            return partition_date < cutoff_date
    return False

# Create archive directory if it doesn't exist
def create_archive_directory(archive_path):
    if not os.path.exists(archive_path):
        os.makedirs(archive_path)

# Move partition to archive and remove from table using beeline
def archive_partition(table_name, partition_name):
    archive_path = os.path.join(ARCHIVE_BASE_PATH, table_name)
    create_archive_directory(archive_path)
    print('Archiving partition: {}'.format(partition_name))
    
    # Run beeline command to alter table and drop partition
    drop_partition_command = (
        "beeline -u 'jdbc:hive2://your_hive_server' "
        "-e \"ALTER TABLE {} DROP IF EXISTS PARTITION (date='{}')\""
        .format(table_name, partition_name)
    )
    subprocess.call(drop_partition_command, shell=True)

    # Move the partition directory to the archive location
    partition_path = os.path.join('/path/to/tables', table_name, partition_name)
    shutil.move(partition_path, archive_path)

# Main function to iterate over tables and archive partitions
def archive_old_partitions(table_list_file):
    table_list = read_table_list(table_list_file)
    for table_name in table_list:
        table_path = os.path.join('/path/to/tables', table_name)
        if not os.path.exists(table_path):
            print('Table path not found: {}'.format(table_path))
            continue

        # List partitions in table directory
        partitions = [d for d in os.listdir(table_path) if os.path.isdir(os.path.join(table_path, d))]
        for partition_name in partitions:
            if is_partition_older_than_beeline(table_name, partition_name, RETENTION_YEARS):
                archive_partition(table_name, partition_name)

if __name__ == '__main__':
    # Provide the path to the file containing the list of tables
    table_list_file = '/path/to/table_list.txt'
    archive_old_partitions(table_list_file)
