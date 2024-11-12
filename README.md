# reddit-nosleep

Convert a ZST file to a queryable SQLite database.

1. Download the Reddit ZST compressed archive of the subreddit you're interested in at https://the-eye.eu/redarcs.
2. Decompress the archive (e.g. zstd). The decompressed file is JSON.
3. Rename the file to `nosleep_submissions.json`.
4. Double check the number of submissions: `jq -s length nosleep_submissions.json`
5. Run the conversion script: `/json-to-sqlite.js -i nosleep_submissions.json -o nosleep.sqlite`
6. Open up the SQLite in your favorite editor.
