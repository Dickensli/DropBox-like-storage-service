SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
#getversion
echo "Get Version:"
if $SCRIPTPATH/../java/target/surfstore/bin/runClient  $SCRIPTPATH/../configs/configCentralized.txt getversion file1test.txt ; then
	echo "getversion success!"
else
	echo "getversion failed!"
fi
echo ""
#upload
echo "Upload:"
if  $SCRIPTPATH/../java/target/surfstore/bin/runClient  $SCRIPTPATH/../configs/configCentralized.txt upload  $SCRIPTPATH/../test/file1/file1test.txt ; then
	echo "upload success!"
else
	echo "upload failed!"
fi
echo ""
#getversion
#echo "Get Version:"
#if $SCRIPTPATH/../java/target/surfstore/bin/runClient  $SCRIPTPATH/../configs/configCentralized.txt getversion file1test.txt ; then
#	echo "getversion success!"
#else
#	echo "getversion failed!"
#fi
#echo ""

#download
echo "Download:"
if  $SCRIPTPATH/../java/target/surfstore/bin/runClient  $SCRIPTPATH/../configs/configCentralized.txt download file1test.txt  $SCRIPTPATH/../test/file2 ; then
	echo "download success!"
else
	echo "download failed!"
fi
echo ""
##delete
#echo "Delete:"
#if  $SCRIPTPATH/../java/target/surfstore/bin/runClient  $SCRIPTPATH/../configs/configCentralized.txt delete file1test.txt ; then
#	echo "delete success!"
#else
#	echo "delete failed!"
#fi
