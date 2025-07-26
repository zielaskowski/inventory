#!/bin/bash

# This script fetches stock data, formats it for display in fzf,
# and uses fzf to provide an interactive filter.

# AFTER ANY CHANGE TO COLUMN ORDER OR COLUMN NAMES 
# IN EXPORT FUNCTION, ADJUST FOLLOWING:
# 1. DEV_ID column number
export DEV_ID=2
# 2. DEV_MAN column number
export DEV_MAN=3
# 2. columns width
export WIDTH="4 17 10 8 10 15 15 10 10"
# 3. std column width
export STD_WIDTH=70
# 4. color every other column
export COLOR1="\033[38;5;76m"; # green
export COLOR2="\033[38;5;85m"; # slightly lighter green
#
# The awk script creates a visually clean and aligned display line based on defined widths,
# with alternating colors for each column to improve readability.
#
# fzf is then configured to interpret the ANSI color codes.

# Function to generate the preview content.
# It uses the HEADER environment variable, which must be exported.
# It must be exported to be available to the bash subshell started by fzf.
generate_preview() {
    # The first field (tab-separated) is the original data line.
    original_line=$(echo "$1" | cut -f1)

    # Split values and headers into separate lines.
    values=$(echo "$original_line" | tr '|' '\n')

    # Combine headers and values with a colon, then pipe to awk for formatting.
    # This approach is robust and handles cases where values might contain colons.
    paste -d':' <(echo "$HEADER") <(echo "$values") | awk '{
        # Find the position of the first colon.
        idx = index($0, ":");
        # Extract the header (everything before the colon).
        hdr = substr($0, 1, idx - 1);
        # Extract the value (everything after the colon).
        val = substr($0, idx + 1);
        # Trim leading/trailing whitespace from the value.
        gsub(/^[ \t]+|[ \t]+$/, "", val);
        # Print the formatted line: header is bright white and 30 chars wide.
        printf "\033[97m%-30s\033[0m: %s\n", hdr, val;
    }'
}
export -f generate_preview

# action for FZF to select device id and copy to clipboard
device_copy(){
	# The first field (tab-separated) is the original data line.
	original_line=$(echo "$1" | cut -f1)
	# ADJUST DEV_ID COLUMN NUMBER IF NEEDED
	dev_id=$(echo "$original_line" | \
		awk -F'|' -v dev_id_col="$DEV_ID" '{print $dev_id_col}' | \
		tr -d '\n')
	echo -n "$dev_id" | wl-copy 			# copy to clipboard, wayland only
	notify-send "Copied to clipboard:" "$dev_id"	# notify what copied KDE only
}
export -f device_copy

# action for FZF to remove selected dev from stock
device_remove(){
	# The first field (tab-separated) is the original data line.
	original_line=$(echo "$1" | cut -f1)
	# ADJUST DEV_ID and/or DEV_MAN COLUMN NUMBER IF NEEDED
	dev_id=$(echo "$original_line" | \
		awk -F '|' -v dev_id_col="$DEV_ID" '{print $dev_id_col}' | \
		tr -d '\n')
	dev_man=$(echo "$original_line" | \
		awk -F '|' -v dev_man_col="$DEV_MAN" '{print $dev_man_col}' | \
		tr -d '\n')
	inv stock --use_device_id "$dev_id" --use_device_manufacturer "$dev_man" | \
		xargs -I{} notify-send {}
	# update stock
	inv stock --fzf
}
export -f device_remove

# This function encapsulates the data formatting logic.
# It reads the data file and formats it for fzf.
format_data_for_fzf() {
    cat "$DATA_FILE" | awk -F'|' \
        -v width_str="$WIDTH" \
        -v std_width_str="$STD_WIDTH" \
        -v color1_str="$COLOR1" \
        -v color2_str="$COLOR2" \
    'BEGIN {
        # --- ADJUST COLUMN WIDTHS HERE ---
        # Add or remove numbers to match the number of columns in your output.
        # Each number represents the width of a column.
        split(width_str, widths, " ");
        
        # A default width for any columns beyond the ones specified above.
        default_width = std_width_str;

        # --- DEFINE ALTERNATING COLORS HERE ---
        # ANSI escape codes for colors. You can change these to your preference.
        # For example, "36" for cyan, "33" for yellow, etc.
        color1 = color1_str; # green
        color2 = color2_str; # slightly lighter green
        reset_color = "\033[0m";
    }
    {
        display_line = ""
        # Iterate over each field to build the colored and formatted display line.
        for (i = 1; i <= NF; i++) {
            # Determine the width for the current column.
            width = (i in widths) ? widths[i] : default_width;

            # Choose the color for the current column.
            color = (i % 2) ? color1 : color2;

            # Trim leading/trailing whitespace from the field.
            gsub(/^[ \t]+|[ \t]+$/, "", $i);

            # Format the field with color and fixed width.
            display_line = display_line sprintf("%s%-" width "s%s ", color, $i, reset_color);
        }

        # Print the original line, a tab, and formatted display line.
        print $0 "\t" display_line 
    }'
}
export -f format_data_for_fzf


# Get the data file from the main inventory script.
export DATA_FILE
DATA_FILE=$(inv stock --fzf)

# If the data file is empty or doesn't exist, exit gracefully.
if [ ! -s "$DATA_FILE" ]; then
    echo "No data available."
    exit 0
fi

# Extract the header row (first line) and export it for the preview function.
# The header is transformed from a pipe-delimited string to a newline-separated list.
export HEADER
HEADER=$(head -n 1 "$DATA_FILE" | tr '|' '\n')

# Cat the file and pipe it to awk for formatting and then to fzf.
format_data_for_fzf | fzf --ansi \
	--delimiter='\t' \
	--with-nth=2 \
	--header-lines=1 \
	--preview-window=top:30%:wrap \
	--preview='bash -c "generate_preview \"{}\""' \
	--border='horizontal' \
	--border-label='ctrl-c to copy ID | ctrl-d to delete' \
	--border-label-pos=-1:bottom \
	--bind='ctrl-c:execute-silent(bash -c "device_copy \"{}\"")' \
	--bind='ctrl-d:execute-silent(bash -c "device_remove \"{}\"")+reload-sync(bash -c "format_data_for_fzf")'
	# recognize color codes
	# delimiter
	# display only second column (first is used for preview)
	# keep header line
	# preview location
	# generate_preview(): split each column into new line
	# add border (necessery to allow labels)
	# add lables describing bindkeys
	# label position
	# key bindings
