terraform {
  required_version = ">= 1.0"

  backend "s3" {
    bucket = "ptap-terraform-state"
    key    = "production/terraform.tfstate"
    region = "us-east-1"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  name            = "ptap"
  env             = "prod"
  state_bucket    = "ptap-terraform-state"
  domain          = "propertytaxproject.com"
  github_subjects = ["PropertyTaxProject/ptap:*"]

  tags = {
    project     = local.name
    environment = local.env
  }
}

data "aws_ssm_parameter" "secret_key" {
  name = "/${local.name}/${local.env}/secret_key"
}

data "aws_ssm_parameter" "mail_username" {
  name = "/${local.name}/${local.env}/mail_username"
}

data "aws_ssm_parameter" "mail_password" {
  name = "/${local.name}/${local.env}/mail_password"
}

data "aws_ssm_parameter" "sentry_dsn" {
  name = "/${local.name}/${local.env}/sentry_dsn"
}

data "aws_ssm_parameter" "google_service_account" {
  name = "/${local.name}/${local.env}/google_service_account"
}

data "aws_ssm_parameter" "db_username" {
  name = "/${local.name}/${local.env}/db_username"
}

data "aws_ssm_parameter" "db_password" {
  name = "/${local.name}/${local.env}/db_password"
}

data "aws_ssm_parameter" "google_sheet_sid" {
  name = "/${local.name}/${local.env}/google_sheet_sid"
}

data "aws_ssm_parameter" "ptap_mail" {
  name = "/${local.name}/${local.env}/ptap_mail"
}

data "aws_ssm_parameter" "uofm_mail" {
  name = "/${local.name}/${local.env}/uofm_mail"
}

data "aws_ssm_parameter" "chicago_mail" {
  name = "/${local.name}/${local.env}/chicago_mail"
}

# Use OIDC across multiple environments
module "iam_github_oidc_provider" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-github-oidc-provider"
  version = "5.30.0"
}

resource "aws_iam_policy" "update_access" {
  name = "${local.name}-update-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:*"]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::${local.state_bucket}",
          "arn:aws:s3:::${local.state_bucket}/*"
        ]
      },
      {
        Action = [
          "lambda:*"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${local.name}*",
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${local.name}*:*"
        ]
      },
      {
        Action = [
          "s3:*"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::${local.name}*",
          "arn:aws:s3:::${local.name}*/*"
        ]
      },
    ]
  })

  tags = local.tags
}

resource "aws_iam_policy" "read_access" {
  name = "${local.name}-read-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:CompleteLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:InitiateLayerUpload",
          "ecr:BatchCheckLayerAvailability",
          "ecr:PutImage",
          "ecr:SetRepositoryPolicy"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action   = ["ssm:Get*"],
        Effect   = "Allow",
        Resource = "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${local.name}/*"
      },
      {
        Action   = ["rds:ListTagsForResource"],
        Effect   = "Allow",
        Resource = [module.rds.db_parameter_group_arn]
      },
      {
        Action = [
          "iam:Get*",
          "iam:List*",
          "logs:List*",
          "logs:Describe*",
          "ecr:Get*",
          "ecr:List*",
          "ecr:Describe*",
          "s3:Get*",
          "s3:List*",
          "events:Get*",
          "events:List*",
          "events:Describe*",
          "ec2:Describe*",
          "rds:Describe*",
          "rds:List*",
          "apigateway:GET",
          "route53:Get*",
          "route53:Describe*",
          "route53:List*",
          "acm:Get*",
          "acm:Describe*",
          "acm:List*",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })

  tags = local.tags
}

module "iam_github_oidc_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-github-oidc-role"
  version = "5.30.0"

  name     = "${local.name}-terraform-github-role"
  subjects = local.github_subjects

  policies = {
    UpdateAccess = aws_iam_policy.update_access.arn
    ReadAccess   = aws_iam_policy.read_access.arn
  }

  tags = local.tags
}

data "aws_iam_policy_document" "s3_public" {
  statement {
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions   = ["s3:GetObject"]
    resources = ["${module.s3.s3_bucket_arn}/*"]
  }
}

module "s3" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "3.15.1"

  bucket        = "${local.name}-${local.env}-assets"
  acl           = "public-read"
  attach_policy = true
  policy        = data.aws_iam_policy_document.s3_public.json

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  website = {
    index_document = "index.html"
  }
}

data "aws_iam_policy_document" "s3_uploads_public" {
  statement {
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions   = ["s3:GetObject"]
    resources = ["${module.s3_uploads.s3_bucket_arn}/*"]
  }
}

module "s3_uploads" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "3.15.1"

  bucket        = "${local.name}-${local.env}-uploads"
  acl           = "public-read"
  attach_policy = true
  policy        = data.aws_iam_policy_document.s3_uploads_public.json

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false

  control_object_ownership = true
  object_ownership         = "BucketOwnerPreferred"

  cors_rule = [
    {
      allowed_methods = ["HEAD", "GET", "PUT", "POST"]
      allowed_headers = ["*"]
      allowed_origins = ["*"]
      max_age_seconds = 3600
    }
  ]
}

# Use for both environments
module "ecr" {
  source  = "terraform-aws-modules/ecr/aws"
  version = "1.6.0"

  repository_name = local.name
  repository_type = "private"

  repository_read_write_access_arns = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
  repository_lifecycle_policy = jsonencode({
    rules = [
      {
        rulePriority = 1,
        description  = "Keep last 10 prod images",
        selection = {
          tagStatus     = "tagged",
          tagPrefixList = ["prod-"],
          countType     = "imageCountMoreThan",
          countNumber   = 10
        },
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2,
        description  = "Keep last 10 dev images",
        selection = {
          tagStatus     = "tagged",
          tagPrefixList = ["dev-"],
          countType     = "imageCountMoreThan",
          countNumber   = 10
        },
        action = {
          type = "expire"
        }
      }
    ]
  })

  tags = local.tags
}

module "lambda" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "6.0.1"

  function_name  = "${local.name}-${local.env}"
  package_type   = "Image"
  create_package = false
  publish        = true
  timeout        = 30
  memory_size    = 1024

  image_uri = "${module.ecr.repository_url}:${var.lambda_image_tag}"

  attach_policy_json = true
  policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Action"   = ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:PutObjectAcl"],
        "Effect"   = "Allow",
        "Resource" = [module.s3_uploads.s3_bucket_arn, "${module.s3_uploads.s3_bucket_arn}/*"]
      }
    ]
  })

  allowed_triggers = {
    AllowExecutionFromAPIGateway = {
      service = "apigateway"
      arn     = "${module.apigw.apigatewayv2_api_execution_arn}/*/*/*"
    },
    AllowExecutionFromCloudWatch = {
      principal  = "events.amazonaws.com"
      source_arn = aws_cloudwatch_event_rule.keep_warm.arn
    }
  }

  environment_variables = {
    ENVIRONMENT             = local.env
    SECRET_KEY              = data.aws_ssm_parameter.secret_key.value
    MAIL_SERVER             = "email-smtp.us-east-1.amazonaws.com"
    MAIL_PORT               = 587
    MAIL_USERNAME           = data.aws_ssm_parameter.mail_username.value
    MAIL_PASSWORD           = data.aws_ssm_parameter.mail_password.value
    SENTRY_DSN              = data.aws_ssm_parameter.sentry_dsn.value
    GOOGLE_SERVICE_ACCCOUNT = data.aws_ssm_parameter.google_service_account.value
    GOOGLE_SHEET_SID        = data.aws_ssm_parameter.google_sheet_sid.value
    S3_UPLOADS_BUCKET       = module.s3_uploads.s3_bucket_id
    DATABASE_URL            = "postgresql+psycopg2://${data.aws_ssm_parameter.db_username.value}:${data.aws_ssm_parameter.db_password.value}@${module.rds.db_instance_endpoint}/${module.rds.db_instance_name}"
    MAIL_DEFAULT_SENDER     = "mail@${local.domain}"
    PTAP_MAIL               = data.aws_ssm_parameter.ptap_mail.value
    UOFM_MAIL               = data.aws_ssm_parameter.uofm_mail.value
    CHICAGO_MAIL            = data.aws_ssm_parameter.chicago_mail.value
    # GOOGLE_LOGGING_ENABLED  = "true"
  }

  tags = local.tags
}

resource "aws_cloudwatch_event_rule" "keep_warm" {
  name                = "${local.name}-${local.env}-keep-lambda-warm"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "keep_warm" {
  rule      = aws_cloudwatch_event_rule.keep_warm.name
  target_id = "${local.name}-${local.env}"
  arn       = module.lambda.lambda_function_arn
}

data "aws_route53_zone" "domain" {
  name = local.domain
}

resource "aws_route53_record" "api" {
  zone_id = data.aws_route53_zone.domain.zone_id
  name    = "app.${local.domain}"
  type    = "A"

  alias {
    name    = module.apigw.apigatewayv2_domain_name_target_domain_name
    zone_id = module.apigw.apigatewayv2_domain_name_hosted_zone_id

    evaluate_target_health = false
  }
}

module "acm" {
  source  = "terraform-aws-modules/acm/aws"
  version = "~> 4.0"

  domain_name = local.domain
  zone_id     = data.aws_route53_zone.domain.zone_id

  validation_method = "DNS"

  subject_alternative_names = [
    "*.${local.domain}"
  ]

  wait_for_validation = true

  tags = local.tags
}

module "apigw" {
  source  = "terraform-aws-modules/apigateway-v2/aws"
  version = "2.2.2"

  name          = "${local.name}-${local.env}"
  protocol_type = "HTTP"

  cors_configuration = {
    allow_headers = ["content-type", "x-amz-date", "authorization", "x-api-key", "x-amz-security-token", "x-amz-user-agent"]
    allow_methods = ["*"]
    allow_origins = ["*"]
  }

  domain_name                 = "app.${local.domain}"
  domain_name_certificate_arn = module.acm.acm_certificate_arn

  # payload_format_version 1.0 is needed for awsgi
  integrations = {
    "ANY /{proxy+}" = {
      lambda_arn             = module.lambda.lambda_function_arn
      payload_format_version = "1.0"
      timeout_milliseconds   = 30000
    }

    "$default" = {
      lambda_arn             = module.lambda.lambda_function_arn
      payload_format_version = "1.0"
      timeout_milliseconds   = 30000
    }
  }

  tags = local.tags
}

# Reuse for multiple environments
data "aws_availability_zones" "available" {}

locals {
  vpc_cidr = "10.0.0.0/16"
  azs      = slice(data.aws_availability_zones.available.names, 0, 3)
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = local.name
  cidr = local.vpc_cidr

  azs              = local.azs
  public_subnets   = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k)]
  private_subnets  = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k + 3)]
  database_subnets = [for k, v in local.azs : cidrsubnet(local.vpc_cidr, 8, k + 6)]

  create_database_subnet_group           = true
  create_database_subnet_route_table     = true
  create_database_internet_gateway_route = true

  tags = local.tags
}

module "security_group" {
  source  = "terraform-aws-modules/security-group/aws//modules/postgresql"
  version = "~> 5.0"

  name                = local.name
  vpc_id              = module.vpc.vpc_id
  ingress_cidr_blocks = ["0.0.0.0/0"]

  tags = local.tags
}

module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "6.1.1"

  identifier = local.name

  engine               = "postgres"
  engine_version       = "14"
  family               = "postgres14"
  major_engine_version = "14"
  instance_class       = "db.t4g.micro"

  allocated_storage     = 5
  max_allocated_storage = 20

  db_name  = local.name
  port     = 5432
  username = data.aws_ssm_parameter.db_username.value
  password = data.aws_ssm_parameter.db_password.value

  manage_master_user_password = false

  multi_az               = false
  db_subnet_group_name   = module.vpc.database_subnet_group
  vpc_security_group_ids = [module.security_group.security_group_id]
  publicly_accessible    = true
  ca_cert_identifier     = "rds-ca-rsa4096-g1"

  maintenance_window              = "Mon:00:00-Mon:03:00"
  backup_window                   = "03:00-06:00"
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]
  create_cloudwatch_log_group     = true

  backup_retention_period = 1
  skip_final_snapshot     = true
  deletion_protection     = false

  performance_insights_enabled          = true
  performance_insights_retention_period = 7
  create_monitoring_role                = true
  monitoring_interval                   = 60

  apply_immediately = true

  parameters = [
    {
      name  = "autovacuum"
      value = 1
    },
    {
      name  = "client_encoding"
      value = "utf8"
    }
  ]

  monitoring_role_name            = "${local.name}-rds-monitoring-role-name"
  monitoring_role_use_name_prefix = true
}

resource "aws_iam_user" "mail" {
  name = "${local.name}-mail"
  tags = local.tags
}

resource "aws_iam_user_policy_attachment" "send_mail" {
  policy_arn = aws_iam_policy.send_mail.arn
  user       = aws_iam_user.mail.name
}

resource "aws_iam_policy" "send_mail" {
  name   = "${local.name}-send-mail"
  policy = data.aws_iam_policy_document.send_mail.json
  tags   = local.tags
}

data "aws_iam_policy_document" "send_mail" {
  statement {
    actions   = ["ses:SendRawEmail"]
    resources = ["*"]
  }
}
