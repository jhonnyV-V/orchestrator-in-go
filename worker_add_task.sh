curl -v --request POST \
--header 'Content-Type: application/json' \
--data '{
"ID": "266592cd-960d-4091-981c-8c25c44b1018",
"State": 2,
"Task": {
"State": 1,
"ID": "266592cd-960d-4091-981c-8c25c44b1018",
"Name": "test-chapter-5-1",
"Image": "strm/helloworld-http"
}
}' localhost:8089/tasks

