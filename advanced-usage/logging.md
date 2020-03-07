[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Logging
Boto3 Batch Utils initialises a logger. The name used is `boto3-batch-utils`.

The simplest method to access the logs for Boto3 Batch Utils is as follows:
```python
import logging

logging.getLogger("boto3-batch-utils").setLevel("WARNING")
```

If more complex interaction with the logger is required, refer to the 
[Python Logging documentation](https://docs.python.org/3.7/library/logging.html)


---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)