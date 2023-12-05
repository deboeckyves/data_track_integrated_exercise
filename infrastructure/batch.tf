resource "aws_batch_job_definition" "ingest-job-definition" {
  name = "yves-integrated-exercise-ingest-tf"
  type = "container"
  timeout {
    attempt_duration_seconds = 1800
  }
  container_properties = jsonencode({
    command = ["python","./ingest.py","-d","2023-11-23","-e","dev"],
    image   = "167698347898.dkr.ecr.eu-west-1.amazonaws.com/yves-integrated-exercise-ingest-tf:latest"
    jobRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    executionRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]

  })
}

resource "aws_batch_job_definition" "transform-job-definition" {
  name = "yves-integrated-exercise-transform-tf"
  type = "container"
  timeout {
    attempt_duration_seconds = 1800
  }
  container_properties = jsonencode({
    command = ["python3","./transform.py","-d","2023-11-23","-e","dev"],
    image   = "167698347898.dkr.ecr.eu-west-1.amazonaws.com/yves-integrated-exercise-transform-tf:v2"
    jobRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    executionRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]

  })
}



resource "aws_batch_job_definition" "load-job-definition" {
  name = "yves-integrated-exercise-load-tf"
  type = "container"
  timeout {
    attempt_duration_seconds = 1800
  }
  container_properties = jsonencode({
    command = ["python3","./load.py","-d","2023-11-23","-e","dev"],
    image   = "167698347898.dkr.ecr.eu-west-1.amazonaws.com/yves-integrated-exercise-load-tf:v2"
    jobRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    executionRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]
  })
}