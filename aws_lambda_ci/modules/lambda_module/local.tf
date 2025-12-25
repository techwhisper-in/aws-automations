locals {
  handler_list = split(".",var.lambda_function_handler)
  handler_function_name=local.handler_list[length(local.handler_list)-1]
  items_except_last = slice(local.handler_list, 0, length(local.handler_list) - 1)
  path_of_main_file=join("/", local.items_except_last)


  python_content="def ${local.handler_function_name}(event, context):\n    return {'statusCode': 200,'body': 'Hello from AWS Lambda!'}"
  js-content="exports.${local.handler_function_name} = async () => {return {statusCode: 200,body: 'Hello from AWS Lambda!'};};"
  java-content="some-code"
}

resource "local_file" "lambda_code" {
    content = (length(regexall("python",var.lambda_runtime_environment))>0)==true ? local.python_content : (length(regexall("python",var.lambda_runtime_environment))>0)==true ? local.js-content : local.java-content
    #filename = "${path.module}/parent_folder/${local.path_of_main_file}.py"
  filename=(length(regexall("python",var.lambda_runtime_environment))>0)==true ? "${path.module}/parent_folder/${local.path_of_main_file}.py" : (length(regexall("python",var.lambda_runtime_environment))>0)==true ? "${path.module}/parent_folder/${local.path_of_main_file}.js" : "${path.module}/parent_folder/${local.path_of_main_file}"
}

data "archive_file" "lambda_zip" {
    type = "zip"
    source_dir = "${path.module}/parent_folder"
    output_path = "${path.module}/lambda_function_payload.zip"
    depends_on = [ local_file.lambda_code ]
}