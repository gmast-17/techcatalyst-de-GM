### Activity 1: Searching for Specific Words in Product Reviews

- **Objective:** Find all lines in `review.csv` that mention the word "quality".

- Command:

  ```sh
  !grep "quality" review.csv
  ```

### Activity 2: Case-Insensitive Search in Hamlet

- **Objective:** Find all lines in `hamlet.txt` that contain the word "francisco" regardless of case.

- Command:

  ```sh
  !grep -i "francisco" hamlet.txt 
  ```

### Activity 3: Counting Occurrences

- **Objective:** Count how many times the word "battery" (case insensitive) appears in `review.csv`.

- Command:

  ```sh
  !grep -i -c "battery" review.csv 
  ```

### Activity 4: Displaying Line Numbers

- **Objective:** Search for the word "excellent" in `review.csv` and display the line numbers of the matches.

- Command:

  ```sh
  !grep -i -n "excellent" review.csv 
  ```

### Activity 5: Inverted Search in Hamlet

- **Objective:** Find all lines in `hamlet.txt` that do not contain the word "the".

- Command:

  ```sh
  !grep -v "the" hamlet.txt 
  ```

### Activity 6: Using Extended Regular Expressions

- **Objective:** Find all lines in `review.csv` that contain the word "design" or "performance".

- Command:

  ```sh
  !grep -e "design" -e"performance" review.csv 
  ```

### Activity 7: Extracting Matching Parts Only

- **Objective:** Extract and display only the words "battery" or "Battery" from `review.csv`.

- Command:

  ```sh
  !grep -o -w '\b[Bb]attery\b' review.csv  
  ```

### Activity 8: Combining `grep` with `find`

- **Objective:** Use `find` to locate all `.txt` files in the `GREP` directory and search for lines containing "sick" in those files.

- Command:

  ```sh
  !find . -name "*.txt" -exec grep "sick" {} +
  ```

### Activity 9: Searching for Patterns in Product Reviews

- **Objective:** Find all lines in `review.csv` where a word starts with "b" and ends with "y".

- Command:

  ```sh
  !grep "b.y" review.csv
  ```