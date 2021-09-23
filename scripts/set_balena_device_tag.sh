TAG_KEY=$1
TAG_VALUE=$2

BALENA_DEVICE_ID=$(curl -sSL "$BALENA_API_URL/v3/device?\$select=id,uuid&\$filter=uuid%20eq%20'$BALENA_DEVICE_UUID'" -H "Authorization: Bearer $BALENA_API_KEY" | jq '.d[0].id')

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH
TAG_EXISTS=$(sh get_device_tag.sh "$TAG_KEY" "$BALENA_DEVICE_ID" | jq ".d[0] != null")

if [ "$TAG_EXISTS" = "false" ] ; then
	curl -sSL -X POST "$BALENA_API_URL/v3/device_tag" \
		-H "Authorization: Bearer $BALENA_API_KEY" \
		-H "Content-Type: application/json" \
		--data-binary '{"tag_key":"'$TAG_KEY'","device":'$BALENA_DEVICE_ID',"value":"'$TAG_VALUE'"}'
else
	curl -sSL -X PATCH "$BALENA_API_URL/v3/device_tag?\$filter=tag_key%20eq%20'$TAG_KEY'" \
		-H "Authorization: Bearer $BALENA_API_KEY" \
		-H "Content-Type: application/json" \
		--data-binary '{"tag_key":"'$TAG_KEY'", "device":'$BALENA_DEVICE_ID', "value":"'$TAG_VALUE'"}'
fi