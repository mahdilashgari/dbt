#!/bin/bash

# Start date
start_date="2021-01-01"

# End date
end_date="2024-03-01"

# Convert start and end dates to seconds since the epoch
start_date_sec=$(date -d "$start_date" +%s)
end_date_sec=$(date -d "$end_date" +%s)

# Initialize a flag for the first run
first_run=true

# Array of months to skip
skip_months=("2021-01" "2021-02" "2021-03" "2022-01" "2022-02" "2022-03" "2022-04" "2022-05" "2022-06")

# Loop from start date to end date, incrementing one month at a time
while [ "$start_date_sec" -le "$end_date_sec" ]; do
    # Convert the current timestamp back to a readable date format for the variable
    current_date=$(date -d "@$start_date_sec" +%Y-%m-%d)
    # Extract year-month for skipping check
    current_year_month=$(date -d "@$start_date_sec" +%Y-%m)

    # Check if current year-month is in the skip list
    if printf '%s\n' "${skip_months[@]}" | grep -qx "$current_year_month"; then
        echo "Skipping $current_year_month"
    else
        # Execute the dbt run command without --full-refresh for subsequent runs
        echo "Running dbt for $current_date"
        dbt run --select central_dim_xing_users_hst_v2 --target=prod --cache-selected-only --vars "{\"date_id\":\"$current_date\"}"
    fi

    # Increment the month by 1
    start_date_sec=$(date -d "$(date -d "@$start_date_sec" +%Y-%m-01) + 1 month" +%s)
done
