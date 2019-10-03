#!/bin/bash

echo -e "Enter database name- \c"
read db
echo -e "Enter table name- \c"
read table
echo -e "Enter data file name- \c"
read file

python create_table.py --db=$db --table=$table
status=$?


if [ $status -eq 0 ]

then
  echo "Table created"
  python insert_data.py --db=$db --table=$table --data=$file
  status=$?

  if [ $status -eq 0 ]
  then
    echo "Insertion successfull"
  else
    echo "Insertion failed"
  fi

else
  echo "Table creation failed. Try again"
fi
