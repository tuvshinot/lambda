# Lambda layer source
**Common Layer for both film-array-xml and sciex-write-mysql functions**

_**How to zip lambda layer**_

`pip install -t ./layer/python/lib/python3.8/site-packages/ pymysql`

`zip -r9 ./layer/python python.zip`

_**How to zip lambda**_
`zip -r9 'FUNCTION_FOLDER/' lambda_function.zip`