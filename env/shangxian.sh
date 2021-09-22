rm -rf bak
mkdir bak
mv bin bak/
mv conf bak/
scp -i /home/ec2-user/tools/keys/mengmai.pem -r ec2-user@ec2-35-154-68-167.ap-south-1.compute.amazonaws.com:/home/ec2-user/mx-beta/server/conf .
scp -i /home/ec2-user/tools/keys/mengmai.pem -r ec2-user@ec2-35-154-68-167.ap-south-1.compute.amazonaws.com:/home/ec2-user/mx-beta/server/bin .
