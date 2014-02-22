#! /bin/bash

ssh root@${1} 'cat .ssh/id_rsa.pub'
