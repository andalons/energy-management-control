# Trigger Configuration for OMIE Pipelines

## Daily Pipeline Trigger

```json
{
  "name": "omie_daily_schedule_trigger",
  "type": "ScheduleTrigger",
  "properties": {
    "description": "Daily trigger for OMIE data ingestion at 6:00 AM UTC",
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "omie_daily_pipeline",
          "type": "PipelineReference"
        },
        "parameters": {
          "maxFilesPerRun": 50,
          "forceReprocessDays": 7
        }
      }
    ],
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "2024-01-01T06:00:00.000Z",
        "timeZone": "UTC",
        "schedule": {
          "hours": [6],
          "minutes": [0]
        }
      }
    },
    "runtimeState": "Started"
  }
}
```

## Monthly Pipeline Trigger

```json
{
  "name": "omie_monthly_maintenance_trigger",
  "type": "ScheduleTrigger",
  "properties": {
    "description": "Monthly trigger for OMIE maintenance on first Sunday at 2:00 AM UTC",
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "omie_monthly_maintenance",
          "type": "PipelineReference"
        },
        "parameters": {
          "validationLookbackDays": 90,
          "enableVacuum": true,
          "vacuumRetentionHours": 168
        }
      }
    ],
    "typeProperties": {
      "recurrence": {
        "frequency": "Month",
        "interval": 1,
        "startTime": "2024-01-07T02:00:00.000Z",
        "timeZone": "UTC",
        "schedule": {
          "hours": [2],
          "minutes": [0],
          "weekDays": ["Sunday"],
          "monthlyOccurrences": [
            {
              "day": "Sunday",
              "occurrence": 1
            }
          ]
        }
      }
    },
    "runtimeState": "Started"
  }
}
```

## Manual Triggers

### Daily Pipeline Manual Trigger

For ad-hoc runs or catch-up processing:

```json
{
  "name": "omie_daily_manual_trigger",
  "type": "TumblingWindowTrigger",
  "properties": {
    "description": "Manual trigger for OMIE daily pipeline with tumbling window support",
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "omie_daily_pipeline",
          "type": "PipelineReference"
        },
        "parameters": {
          "maxFilesPerRun": 100,
          "forceReprocessDays": 30
        }
      }
    ],
    "typeProperties": {
      "frequency": "Day",
      "interval": 1,
      "startTime": "2024-01-01T00:00:00.000Z",
      "endTime": "2025-12-31T23:59:59.999Z",
      "delay": "00:00:00",
      "maxConcurrency": 1,
      "retryPolicy": {
        "count": 2,
        "intervalInSeconds": 300
      }
    },
    "runtimeState": "Stopped"
  }
}
```

## Event-Based Triggers

### Data Availability Trigger

Trigger when new data is detected:

```json
{
  "name": "omie_data_available_trigger",
  "type": "CustomEventsTrigger",
  "properties": {
    "description": "Trigger when new OMIE data becomes available",
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "omie_daily_pipeline",
          "type": "PipelineReference"
        },
        "parameters": {
          "maxFilesPerRun": 25,
          "forceReprocessDays": 3
        }
      }
    ],
    "typeProperties": {
      "events": [
        {
          "eventType": "Microsoft.EventGrid.SubscriptionValidationEvent"
        },
        {
          "eventType": "omie.data.available"
        }
      ],
      "scope": "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.EventGrid/topics/omie-events"
    },
    "runtimeState": "Stopped"
  }
}
```

## Trigger Dependencies

### Sequential Monthly Maintenance

Ensure monthly maintenance runs after successful daily processing:

```json
{
  "name": "omie_monthly_dependent_trigger",
  "type": "TumblingWindowTrigger",
  "properties": {
    "description": "Monthly maintenance trigger with dependency on daily pipeline",
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "omie_monthly_maintenance",
          "type": "PipelineReference"
        }
      }
    ],
    "typeProperties": {
      "frequency": "Month",
      "interval": 1,
      "startTime": "2024-01-01T02:00:00.000Z",
      "delay": "00:00:00",
      "maxConcurrency": 1,
      "dependsOn": [
        {
          "type": "TumblingWindowTriggerDependencyReference",
          "referenceName": "omie_daily_manual_trigger",
          "offset": "-1.00:00:00",
          "size": "1.00:00:00"
        }
      ]
    },
    "runtimeState": "Started"
  }
}
```

## Configuration Notes

### Time Zone Considerations

- All triggers use UTC timezone
- Adjust for local time zones as needed
- Consider daylight saving time changes

### Concurrency Settings

- Daily pipeline: Max 2 concurrent runs
- Monthly maintenance: Max 1 concurrent run
- Manual triggers: Max 1 concurrent run

### Retry Policies

- Daily pipeline: 2 retries with 5-minute intervals
- Monthly maintenance: 1 retry with 10-minute intervals
- Critical failures: Immediate alerts

### Resource Allocation

Triggers should consider:

- Fabric capacity availability
- Peak usage hours
- Other pipeline schedules
- Maintenance windows

### Monitoring Integration

All triggers include:

- Success/failure notifications
- Performance metrics collection
- Error logging and alerting
- Dependency tracking
