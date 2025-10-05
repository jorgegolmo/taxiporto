
# Setup

First follow the assignment's setup instructions up to the Python requirements installation. Then, for the database connector to work properly, you need to add the following `.env` file inside the **git root** directory:

```
HOSTNAME=$(your computer/container's hostname)
DATABASE=$(the name of the database you created)
USERNAME=$(your MySQL username)
PASSWORD=$(your MySQL password)
```

Lastly, you need to have Git LFS installed so as to download porto.csv. If you have not installed it before cloning the repo, you must do `git lfs pull` to fetch the file.
