
rm ./pixels/pixels.properties
cp pixels.properties ./pixels
zip -r pixels.zip ./pixels

aws s3 cp pixels.zip s3://jingrong-test/packages/
aws s3 cp pixels-turbo/pixels-worker-lambda/target/pixels-worker-lambda-deps.zip s3://jingrong-test/packages/
aws s3 cp pixels-turbo/pixels-worker-lambda/target/pixels-worker-lambda.jar s3://jingrong-test/packages/