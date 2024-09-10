import pandas as pd
from datetime import datetime

# Sample data (replace this with your actual DataFrame)
data = {'parm_name': ['startedTime', 'finishedTime', 'otherParam'],
        'parm_val': ['2023-09-11 10:30:00', '2023-09-11 12:30:00', 'someValue']}

df = pd.DataFrame(data)

# Filter for the 'startedTime' and 'finishedTime' rows
start_time = df[df['parm_name'] == 'startedTime']['parm_val'].values[0]
finish_time = df[df['parm_name'] == 'finishedTime']['parm_val'].values[0]

# Convert to datetime format
start_time = pd.to_datetime(start_time)
finish_time = pd.to_datetime(finish_time)

# Calculate the time difference
time_difference = finish_time - start_time

# Store the result in a new DataFrame
result_df = pd.DataFrame({
    'startedTime': [start_time],
    'finishedTime': [finish_time],
    'timeDifference': [time_difference]
})

# Display the result
print(result_df)
