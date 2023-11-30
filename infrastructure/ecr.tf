


resource "aws_ecr_repository" "ingest-repo" {

  name                 = "yves-integrated-exercise-ingest-tf"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "transform-repo" {

  name                 = "yves-integrated-exercise-transform-tf"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "load-repo" {

  name                 = "yves-integrated-exercise-load-tf"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}