.. Boto3 Batch Utils documentation master file, created by
   sphinx-quickstart on Sat Feb  8 10:47:02 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Boto3 Batch Utils's documentation!
=============================================

Boto3 Batch Utils is a Python library which helps to make interactions with AWS Services more efficient, by batching
requests.

For example, when writing records to Kinesis, you may use boto3's method ``put_record``. However, ig you need to send
multiple records to Kinesis, using ``put_record`` is inefficient. Boto3 provides a ``put_records`` method, which makes
the sending of multiple records more efficient. However, that requires you to manage the lists of records yourself.

Using a Boto3 Batch Utils client enables you to avoid the complexity of managing batch sizes, payload sizes and
split/retry patterns.

.. toctree::
   :maxdepth: 2
   :caption: Installation:

   pages/installation.md
   pages/supported_aws_services.md


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
