# Fabric Data Factory Pipeline Definitions for OMIE Data Processing

This directory contains the pipeline definitions and configuration files for Microsoft Fabric Data Factory to orchestrate OMIE data processing.

## Pipeline Overview

### Daily Pipeline (`omie_daily_pipeline`)

- **Schedule**: Daily at 6:00 AM UTC
- **Purpose**: Incremental data ingestion
- **Duration**: ~15-30 minutes
- **Triggers**: Schedule + Manual

### Monthly Pipeline (`omie_monthly_maintenance`)

- **Schedule**: First Sunday of each month at 2:00 AM UTC
- **Purpose**: Data validation, optimization, reporting
- **Duration**: ~1-2 hours
- **Triggers**: Schedule + Manual

## File Structure

```
pipelines/
├── daily/
│   ├── omie_daily_pipeline.json         # Main daily pipeline definition
│   ├── daily_parameters.json            # Pipeline parameters
│   └── daily_triggers.json              # Schedule and triggers
├── monthly/
│   ├── omie_monthly_maintenance.json    # Monthly maintenance pipeline
│   ├── monthly_parameters.json          # Pipeline parameters
│   └── monthly_triggers.json            # Schedule and triggers
├── shared/
│   ├── linked_services.json             # External connections
│   ├── datasets.json                    # Data source definitions
│   └── common_parameters.json           # Shared parameters
└── deployment/
    ├── deploy_pipelines.ps1             # PowerShell deployment script
    ├── validate_pipelines.py            # Pipeline validation
    └── monitoring_setup.json            # Monitoring configuration
```

## Deployment Instructions

1. **Prerequisites**:

   - Microsoft Fabric workspace with Premium capacity
   - Lakehouse `lkh_OMIE` created
   - Service principal with appropriate permissions

2. **Deploy pipelines**:

   ```powershell
   .\deployment\deploy_pipelines.ps1
   ```

3. **Validate deployment**:

   ```python
   python deployment/validate_pipelines.py
   ```

4. **Enable monitoring**:
   - Import monitoring configuration
   - Set up alerts and notifications

## Configuration

### Environment Variables

Set these in your Fabric environment:

- `OMIE_LAKEHOUSE_NAME`: Target lakehouse name (default: lkh_OMIE)
- `OMIE_NOTIFICATION_EMAIL`: Email for alerts
- `OMIE_MAX_RETRY_COUNT`: Max retries for failed activities (default: 3)
- `OMIE_TIMEOUT_MINUTES`: Activity timeout (default: 60)

### Pipeline Parameters

Key parameters that can be overridden:

- `target_years`: Array of years to process (default: [2023,2024,2025])
- `max_files_per_run`: Limit files per execution (default: 50)
- `force_reprocess_days`: Days to force reprocessing (default: 7)
- `enable_optimization`: Enable Delta optimization (default: true)

## Monitoring and Alerts

### Key Metrics

- Pipeline success/failure rates
- Data volume processed
- Processing duration
- Error frequency
- Data quality scores

### Alert Conditions

- Pipeline failure
- Data quality below threshold
- Processing time exceeds SLA
- No new data for >48 hours
- Critical errors in monthly maintenance

## Troubleshooting

### Common Issues

1. **Pipeline fails with authentication error**:

   - Check service principal permissions
   - Verify lakehouse access rights

2. **No new files detected**:

   - Verify OMIE website accessibility
   - Check network connectivity
   - Review file discovery logic

3. **Delta table write failures**:

   - Check lakehouse storage capacity
   - Verify Delta table permissions
   - Review schema compatibility

4. **Monthly maintenance timeout**:
   - Increase timeout settings
   - Check optimization settings
   - Review data volume growth

### Support Contacts

- Data Engineering Team: data-eng@company.com
- Fabric Admin: fabric-admin@company.com
- On-call Support: +1-xxx-xxx-xxxx
