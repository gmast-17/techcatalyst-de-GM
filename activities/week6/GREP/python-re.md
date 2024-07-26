### Activity: Extract Dates, Phone Numbers, and Emails from Text

#### Instructions

1. **Objective:** Use regex to extract dates, phone numbers, and email addresses from a given text.
2. **Tools:** Python and the `re` module.
3. Tasks:
   - Write a regex pattern to extract dates in the format `YYYY-MM-DD`.
   - Write a regex pattern to extract phone numbers in the format `123-456-7890`.
   - Write a regex pattern to extract email addresses.

#### Sample Text

```python
email_text = """
Hi Team,

Please note the following updates for our project:

1. The next meeting is scheduled for 2024-08-15 at our main office. Please confirm your availability.
2. We have received a new batch of feedback from clients. Some of the notable ones include:
   - "The service was excellent and the response time was quick."
   - "Please contact me at john.doe@example.com for further discussions."
3. Our support team can be reached at:
   - Phone: 123-456-7890 (John Doe)
   - Phone: 987-654-3210 (Jane Smith)
4. The project deadline has been moved to 2024-12-31. Ensure all deliverables are completed by then.
5. For any urgent issues, please email support@project.com or call our hotline at 555-123-4567.
6. The previous meeting minutes are available at 2023-07-25. Please review them before the next meeting.

Best regards,
Project Manager
"""
```

#### Tasks

1. **Extract Dates:**

   ```python
   # Dates: ['2024-08-15', '2024-12-31', '2023-07-25']
   ```

2. **Extract Phone Numbers:**

   ```python
   # Phone Numbers: ['123-456-7890', '987-654-3210', '555-123-4567']
   ```

3. **Extract Email Addresses:**

   ```python
   # Emails: ['john.doe@example.com', 'support@project.com']
   ```

