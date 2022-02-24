# step 0. create the data folder

mkdir "data/bz2"

# Step 1. Download raw data from a third party dump: https://files.pushshift.io/reddit

# download comments for year 2011
wget https://files.pushshift.io/reddit/comments/RC_2011-01.bz2 -P data/bz2

# download submissions for year 2011
wget https://files.pushshift.io/reddit/submissions/RS_2011-01.zst -P data/bz2

# Step 2. Read the `.bz2` files and group items from the same subreddit 

python src/data_pikabu.py bz2 2011

# Step 3. extract basic attributes and dialog trees.

python src/data_pikabu.py basic 2011

# Step 4. Build training and testing data for different feedback signals. 

python src/data_pikabu.py updown 2011 --year_to=2011
python src/data_pikabu.py depth 2011 --year_to=2011
python src/data_pikabu.py width 2011 --year_to=2011