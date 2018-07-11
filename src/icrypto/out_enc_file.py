# -*- coding: utf-8 -*-

import sys

from Crypto.Cipher import AES
import base64
import hashlib
import struct

#sys.path.insert(0, 'icrypto')
import icrypto

def makeencfile(file_name):
    crypto = icrypto.ICrypto()

    bitcheck = crypto.checkbit()

# ID ENCODE
    idf = open(file_name, 'r')

    idline = idf.readline()
    print(idline)
    idf.close()

    encode_id = crypto.encodeAES(idline)

    print("encodeAES[ID]: {}".format(encode_id))

    if(bitcheck == 32):
        enc_file_name = file_name + "_enc"
    else:
        enc_file_name = file_name + "_enc_64"

    idf2 = open(enc_file_name, 'w')
    idf2.write(encode_id)
    idf2.close()

def main():
  argc = len(sys.argv)

  if(argc==2):
     makeencfile(sys.argv[1])
  else:
     print('[Error]Argument count is wrong. \n')
     #printException (exception)
     exit(1)


if __name__ == "__main__":
    main()
