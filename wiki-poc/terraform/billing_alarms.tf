# ---------------------------------------------------------------------------
# Billing alarms
#
# The AWS/Billing "EstimatedCharges" metric is ONLY published in us-east-1,
# regardless of where your resources live. So these alarms — and the SNS topic
# they notify (alarm actions must be in the same region as the alarm) — are
# created in us-east-1 via an aliased provider.
#
# PREREQUISITE (one-time, manual): In the Billing console enable
# "Receive Billing Alerts" under Billing Preferences. Until that setting is on,
# the EstimatedCharges metric never populates and every alarm below sits in
# INSUFFICIENT_DATA. This cannot be toggled via standard Terraform/API.
# ---------------------------------------------------------------------------

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

variable "total_budget_usd" {
  description = "Threshold (USD) for the total estimated monthly charges alarm."
  type        = number
  default     = 50
}

variable "per_service_budgets_usd" {
  description = <<-EOT
    Per-service estimated-charge thresholds (USD). Keys MUST be the exact
    AWS/Billing ServiceName dimension values. Add/remove services here as your
    footprint changes — each entry produces one alarm via for_each.
  EOT
  type        = map(number)
  default = {
    AmazonECS        = 20  # Fargate compute (the always-on producer task)
    AmazonS3         = 5   # raw + delta buckets
    AmazonDynamoDB   = 2   # tiny on-demand checkpoint table
    AmazonCloudWatch = 10  # custom metrics + logs (the one that just bit us)
    AmazonEC2        = 5   # ENI / data transfer for the Fargate task's public IP
    AmazonECR        = 2   # image storage
    AmazonSNS        = 1   # alert notifications
  }
}

# Dedicated SNS topic in us-east-1 — a CloudWatch alarm can only target an SNS
# topic in its own region, so this is separate from the us-east-2 ops topic.
resource "aws_sns_topic" "billing_alerts" {
  provider = aws.us_east_1
  name     = "wiki-poc-billing-alerts"
}

resource "aws_sns_topic_subscription" "billing_email" {
  provider  = aws.us_east_1
  topic_arn = aws_sns_topic.billing_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Total estimated charges across the whole account.
resource "aws_cloudwatch_metric_alarm" "billing_total" {
  provider            = aws.us_east_1
  alarm_name          = "wiki-poc-billing-total"
  alarm_description   = "Total estimated AWS charges exceeded ${var.total_budget_usd} USD"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  dimensions          = { Currency = "USD" }
  statistic           = "Maximum"
  # Billing metric refreshes only a few times per day; a 6h period is plenty.
  period              = 21600
  evaluation_periods  = 1
  threshold           = var.total_budget_usd
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.billing_alerts.arn]
  ok_actions          = [aws_sns_topic.billing_alerts.arn]
}

# One alarm per service in the map above.
resource "aws_cloudwatch_metric_alarm" "billing_per_service" {
  provider            = aws.us_east_1
  for_each            = var.per_service_budgets_usd
  alarm_name          = "wiki-poc-billing-${each.key}"
  alarm_description   = "Estimated ${each.key} charges exceeded ${each.value} USD"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  dimensions          = { Currency = "USD", ServiceName = each.key }
  statistic           = "Maximum"
  period              = 21600
  evaluation_periods  = 1
  threshold           = each.value
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.billing_alerts.arn]
}
