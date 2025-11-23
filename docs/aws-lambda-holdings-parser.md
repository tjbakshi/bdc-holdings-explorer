# AWS Lambda Holdings Parser Setup Guide

This document provides step-by-step instructions for deploying the heavy Schedule of Investments parser as an AWS Lambda function.

## Overview

The Lambda function:
- Receives a `filingId` via HTTP POST
- Fetches the filing metadata from Supabase
- Downloads the SEC HTML documents
- Parses the Schedule of Investments tables
- Inserts holdings into Supabase
- Returns the number of holdings inserted

## Prerequisites

- AWS Account with access to Lambda and API Gateway
- Supabase project with `filings` and `holdings` tables
- Node.js 18.x or higher for local testing

## Step 1: Create the Lambda Function

### 1.1 Navigate to AWS Lambda Console
1. Log in to [AWS Console](https://console.aws.amazon.com/)
2. Navigate to **Lambda** service
3. Click **Create function**

### 1.2 Configure Basic Settings
- **Function name**: `extract-holdings-parser`
- **Runtime**: Node.js 18.x
- **Architecture**: x86_64
- **Execution role**: Create a new role with basic Lambda permissions

### 1.3 Set Function Configuration
After creating the function:
1. Go to **Configuration** → **General configuration**
2. Click **Edit**
3. Set:
   - **Memory**: 1024 MB (or higher for large filings)
   - **Timeout**: 10 minutes (600 seconds)
4. Click **Save**

## Step 2: Add Dependencies

The Lambda function requires the `jsdom` package for HTML parsing.

### 2.1 Create a deployment package locally

```bash
mkdir lambda-package
cd lambda-package
npm init -y
npm install jsdom
npm install --save-dev @types/aws-lambda @types/node typescript
```

### 2.2 Copy the Lambda handler code

Create `index.ts` in the `lambda-package` directory and copy the code from `lambda/extract_holdings_lambda.ts` in this repository.

### 2.3 Compile TypeScript

```bash
npx tsc index.ts --outDir dist --target es2020 --module commonjs --esModuleInterop
```

### 2.4 Create deployment zip

```bash
cd dist
cp -r ../node_modules .
zip -r lambda-package.zip .
```

### 2.5 Upload to Lambda
1. In the Lambda console, go to **Code** tab
2. Click **Upload from** → **.zip file**
3. Upload `lambda-package.zip`
4. Set **Handler**: `index.handler`

## Step 3: Configure Environment Variables

1. Go to **Configuration** → **Environment variables**
2. Click **Edit**
3. Add the following variables:

| Key | Value | Description |
|-----|-------|-------------|
| `SEC_USER_AGENT` | `BDCTrackerApp/1.0 (your-email@example.com)` | Required by SEC.gov API |
| `SUPABASE_URL` | `https://your-project.supabase.co` | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | `eyJ...` | Your Supabase service role key (secret!) |

4. Click **Save**

⚠️ **Security Note**: The `SUPABASE_SERVICE_ROLE_KEY` has admin access to your database. Keep it secure and never commit it to version control.

## Step 4: Create API Gateway HTTP Endpoint

### 4.1 Create API Gateway
1. Navigate to **API Gateway** in AWS Console
2. Click **Create API**
3. Choose **HTTP API**
4. Click **Build**

### 4.2 Configure Integration
1. **Integration type**: Lambda
2. **Lambda function**: Select `extract-holdings-parser`
3. **API name**: `bdc-holdings-parser-api`
4. Click **Next**

### 4.3 Configure Routes
1. **Method**: POST
2. **Resource path**: `/parse-holdings`
3. Click **Next**

### 4.4 Configure Stages
1. **Stage name**: `prod`
2. **Auto-deploy**: Enabled
3. Click **Next**

### 4.5 Review and Create
1. Review the configuration
2. Click **Create**

### 4.6 Note the Invoke URL
After creation, note the **Invoke URL** displayed at the top of the API Gateway page. It will look like:

```
https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod
```

Your full endpoint will be:
```
https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod/parse-holdings
```

## Step 5: Configure IAM Permissions

The Lambda execution role needs outbound HTTPS access (enabled by default) and no special permissions for Supabase (uses API key).

If you encounter issues:
1. Go to **Lambda** → **Configuration** → **Permissions**
2. Click on the execution role name
3. Verify the role has `AWSLambdaBasicExecutionRole` attached

## Step 6: Test the Lambda Function

### 6.1 Test via Lambda Console

1. In the Lambda console, go to the **Test** tab
2. Create a new test event with this JSON:

```json
{
  "body": "{\"filingId\":\"your-test-filing-id-here\"}"
}
```

3. Click **Test**
4. Check the execution results and logs

### 6.2 Test via API Gateway (curl)

Replace `YOUR_API_ENDPOINT` and `YOUR_FILING_ID` with actual values:

```bash
curl -X POST https://YOUR_API_ENDPOINT/parse-holdings \
  -H "Content-Type: application/json" \
  -d '{"filingId":"YOUR_FILING_ID"}'
```

Expected response:
```json
{
  "filingId": "uuid-here",
  "holdingsInserted": 150,
  "warnings": []
}
```

## Step 7: Add Lambda URL to Supabase Edge Function

1. Copy your API Gateway invoke URL (e.g., `https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod/parse-holdings`)
2. In Lovable, add a new secret named `LAMBDA_PARSER_URL` with this value
3. The Edge function will automatically use Lambda for heavy parsing

## Architecture Diagram

```
┌─────────────┐
│   Admin UI  │
└──────┬──────┘
       │ POST /extract_holdings_for_filing
       ▼
┌─────────────────────────────┐
│  Supabase Edge Function     │
│  (Thin Orchestrator)        │
│  - Validates request        │
│  - Decides: local vs Lambda │
└──────┬──────────────────────┘
       │ If large/default
       │ POST { filingId }
       ▼
┌─────────────────────────────┐
│   AWS Lambda + API Gateway  │
│   - Fetch filing from DB    │
│   - Download SEC HTML       │
│   - Parse SOI tables        │
│   - Insert holdings to DB   │
└─────────────────────────────┘
       │
       ▼
   [Supabase DB]
```

## Monitoring and Logs

### CloudWatch Logs
1. Navigate to **CloudWatch** → **Logs** → **Log groups**
2. Find `/aws/lambda/extract-holdings-parser`
3. View real-time logs for debugging

### Lambda Metrics
1. In Lambda console, go to **Monitor** tab
2. View:
   - Invocations
   - Duration
   - Error count
   - Throttles

## Cost Estimation

Based on AWS Lambda pricing (as of 2024):

- **Requests**: $0.20 per 1M requests
- **Compute**: $0.0000166667 per GB-second

Example scenario:
- 66 BDCs × 4 quarterly filings = 264 filings/year
- Average execution: 30 seconds at 1024 MB
- Cost: ~$0.01 per filing = ~$2.64/year

**Note**: Actual costs may vary based on filing size and execution time.

## Troubleshooting

### Issue: Lambda timeout
**Solution**: Increase timeout to 15 minutes or reduce `maxHoldings` in the Lambda code.

### Issue: Out of memory
**Solution**: Increase memory allocation to 2048 MB or 3008 MB.

### Issue: "Missing Supabase configuration"
**Solution**: Verify `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` environment variables are set correctly.

### Issue: SEC rate limiting
**Solution**: The Lambda includes retry logic. If you hit rate limits frequently, add delays between document fetches.

### Issue: No holdings found
**Solution**: Enable debug mode by setting `debugMode = true` in `parseHtmlScheduleOfInvestments` and check CloudWatch logs for table headers.

## Updating the Lambda Code

When you need to update the parser logic:

1. Edit `lambda/extract_holdings_lambda.ts` locally
2. Recompile and re-create the zip package (see Step 2)
3. Upload the new zip to Lambda
4. Test with a known filing

## Security Best Practices

1. **Never commit secrets**: Keep `SUPABASE_SERVICE_ROLE_KEY` in environment variables only
2. **Rotate keys**: Periodically rotate your Supabase service role key
3. **Monitor usage**: Set up CloudWatch alarms for unusual activity
4. **Use IAM roles**: Don't hardcode AWS credentials in the Lambda code

## Next Steps

After successful deployment:
1. Test with a large filing (e.g., ARCC)
2. Verify holdings appear in Supabase `holdings` table
3. Check Admin UI shows correct parsed status
4. Monitor CloudWatch logs for any issues

For questions or issues, refer to:
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [API Gateway Documentation](https://docs.aws.amazon.com/apigateway/)
- [Supabase REST API Documentation](https://supabase.com/docs/guides/api)
