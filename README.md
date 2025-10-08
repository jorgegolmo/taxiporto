
# Project Setup

First follow the assignment's setup instructions up to the Python requirements installation. Then, for the database connector to work properly, you need to add the following `.env` file inside the **git root** directory:

```sh
HOSTNAME=$(your computer/container's hostname)
DATABASE=$(the name of the database you created)
USERNAME=$(your MySQL username)
PASSWORD=$(your MySQL password)
```

Lastly, you need to have Git LFS installed so as to download porto.csv. If you have not installed it before cloning the repo, you must do `git lfs pull` to fetch the file.

# EDA and Database Setup

The EDA is contained in eda.ipynb, which also generates a new CSV stripped of useless and invalid data. You must run it (e.g. via `jupyter execute eda.ipynb`) in order to populate the database because the clean CSV is used in that process.

To create the database tables, first run `\. init.sql` inside of the MySQL console and then, outside of it, run `python insert.py` to populate them. Please note that the insertion process takes up a long time (~30-45min).
