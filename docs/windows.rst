Windows
=======

Local paths and file URIs
-------------------------

megfile accepts native Windows paths and its own ``file://`` syntax.
On Windows, ``FSPath(path).as_uri()`` produces the following forms.

.. list-table::
   :header-rows: 1

   * - Path type
     - Windows path
     - megfile URI
   * - Absolute path on a drive
     - ``C:\data\report.txt``
     - ``file:///C:/data/report.txt``
   * - UNC network share
     - ``\\server\share\report.txt``
     - ``file:////server/share/report.txt``
   * - Relative to the working directory
     - ``data\report.txt``
     - ``file://data/report.txt``

For a network share, keep all four slashes after ``file:``. The first pair
belongs to the prefix and the second pair starts the UNC path. With only two
slashes, megfile reads ``file://server/share/report.txt`` as a relative path
under a local directory named ``server``.

Downloading from S3
-------------------

S3 object keys are case-sensitive. In a default Windows directory,
``reports/A.txt`` and ``reports/a.txt`` refer to the same local file.
When downloading both objects, ``smart_sync`` can keep only one local copy.
``smart_move`` can then remove both source objects from S3 after the sync
completes. To keep both files, use a case-sensitive destination directory
or download the objects under distinct local names.
