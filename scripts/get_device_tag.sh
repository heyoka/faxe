TAG_KEY=$1
BALENA_DEVICE_ID=$2
if ! [ "$BALENA_DEVICE_ID" ] ; then
	BALENA_DEVICE_ID=$(curl -sSL "$BALENA_API_URL/v3/device?\$select=id,uuid&\$filter=uuid%20eq%20'$BALENA_DEVICE_UUID'" -H "Authorization: Bearer $BALENA_API_KEY" | jq '.d[0].id')
fi

curl -sSL -X GET "$BALENA_API_URL/v3/device_tag?\$filter=((device%20eq%20$BALENA_DEVICE_ID)%20and%20(tag_key%20eq%20'$TAG_KEY'))" \
	-H "Content-Type:application/json" \
	-H "Authorization: Bearer $BALENA_API_KEY"