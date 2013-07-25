#! /bin/bash

pushd .

echo " "

while read line; do
  firstChar=`echo $line | cut -c1-1`
  if [ "$firstChar" != "#" -a "$firstChar" != "" ] ;
  then
    echo "running:" $line ; eval $line ;
  fi
done < $1

echo " "
echo "Script complete"
popd