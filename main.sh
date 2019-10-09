#!/bin/bash

echo -e "Enter database name- \c"
read db
echo -e "Enter table name- \c"
read table
echo -e "Enter data file name- \c"
read file

python create_table.py --db=$db --table=$table


if [[ $? == "0" ]]
then
  echo "Table created"
  python insert_data.py --db=$db --table=$table --data=$file

  if [[ $? == "0" ]]
  then
    echo "Importing successfull"
  else
    echo "Import failed"
  fi

else
  echo "Table creation failed. Try again"
fi
