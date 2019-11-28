package aws

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/quicksight"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
)

func resourceAwsQuickSightDataSource() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsQuickSightDataSourceCreate,
		Read:   resourceAwsQuickSightDataSourceRead,
		Update: resourceAwsQuickSightDataSourceUpdate,
		Delete: resourceAwsQuickSightDataSourceDelete,

		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"arn": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"aws_account_id": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
				ForceNew: true,
			},

			"credentials": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"credential_pair": {
							Type:     schema.TypeList,
							Required: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"password": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"username": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
					},
				},
			},

			"id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.NoZeroValues,
			},

			"parameters": {
				Type:     schema.TypeList,
				Required: true,
				MaxItems: 1,
				MinItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"amazon_elasticsearch": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"domain": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
						"athena": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"work_group": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
						"aurora": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"aurora_postgre_sql": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"aws_iot_analytics": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"data_set_name": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
						"jira": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"site_base_url": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
						"maria_db": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"mysql": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"postgresql": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"presto": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"catalog": {
										Type:     schema.TypeString,
										Required: true,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						// The documentation is not clear on how to pass RDS parameters...
						"redshift": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"cluster_id": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"port": {
										Type:     schema.TypeInt,
										Optional: true,
									},
								},
							},
						},
						"s3": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"manifest_file_location": {
										Type:     schema.TypeList,
										Required: true,
										MaxItems: 1,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"bucket": {
													Type:         schema.TypeString,
													Required:     true,
													ValidateFunc: validation.NoZeroValues,
												},
												"key": {
													Type:         schema.TypeString,
													Required:     true,
													ValidateFunc: validation.NoZeroValues,
												},
											},
										},
									},
								},
							},
						},
						"service_now": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"site_base_url": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
						"snowflake": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"warehouse": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
						"spark": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"host": {
										Type:     schema.TypeString,
										Required: true,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"sql_server": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"teradata": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"database": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"host": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
									"port": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
								},
							},
						},
						"twitter": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"max_rows": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
									"query": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.NoZeroValues,
									},
								},
							},
						},
					},
				},
			},

			"permission": {
				Type:     schema.TypeList,
				Optional: true,
				MinItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"actions": {
							Type:     schema.TypeSet,
							Required: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
							Set:      schema.HashString,
						},
						"principal": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
					},
				},
			},

			"ssl_properties": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"disable_ssl": {
							Type:     schema.TypeBool,
							Optional: true,
						},
					},
				},
			},

			"tags": tagsSchema(),

			// This will be inferred from the passed `parameters` value
			"type": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"vpc_connection_properties": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"vpc_connection_arn": {
							Type:         schema.TypeBool,
							Optional:     true,
							ValidateFunc: validateArn,
						},
					},
				},
			},
		},
	}
}

func resourceAwsQuickSightDataSourceCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId := meta.(*AWSClient).accountid
	id := d.Get("id").(string)

	if v, ok := d.GetOk("aws_account_id"); ok {
		awsAccountId = v.(string)
	}

	params := &quicksight.CreateDataSourceInput{
		AwsAccountId: aws.String(awsAccountId),
		DataSourceId: aws.String(id),
	}

	if credentials := resourceAwsQuickSightDataSourceCredentials(d); credentials != nil {
		params.Credentials = credentials
	}

	if dataSourceType, dataSourceParameters := resourceAwsQuickSightDataSourceParameters(d); dataSourceParameters != nil {
		params.Type = dataSourceType
		params.DataSourceParameters = dataSourceParameters
		d.Set("type", dataSourceType)
	}

	if v := d.Get("permissions"); v != nil {
		params.Permissions = make([]*quicksight.ResourcePermission, 0)

		for _, v := range v.(*schema.Set).List() {
			permissionResource := v.(map[string]interface{})
			permission := &quicksight.ResourcePermission{
				Actions:   expandStringSet(permissionResource["actions"].(*schema.Set)),
				Principal: aws.String(permissionResource["principal"].(string)),
			}

			params.Permissions = append(params.Permissions, permission)
		}
	}

	if sslProperties := resourceAwsQuickSightDataSourceSslProperties(d); sslProperties != nil {
		params.SslProperties = sslProperties
	}

	if v, ok := d.GetOk("tags"); ok {
		params.Tags = tagsFromMapQuickSight(v.(map[string]interface{}))
	}

	if vpcConnectionProperties := resourceAwsQuickSightDataSourceVpcConnectionProperties(d); vpcConnectionProperties != nil {
		params.VpcConnectionProperties = vpcConnectionProperties
	}

	_, err := conn.CreateDataSource(params)
	if err != nil {
		return fmt.Errorf("Error creating QuickSight Data Source: %s", err)
	}

	d.SetId(fmt.Sprintf("%s/%s", awsAccountId, id))

	return resourceAwsQuickSightDataSourceRead(d, meta)
}

func resourceAwsQuickSightDataSourceRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId, dataSourceId, err := resourceAwsQuickSightDataSourceParseID(d.Id())
	if err != nil {
		return err
	}

	descOpts := &quicksight.DescribeDataSourceInput{
		AwsAccountId: aws.String(awsAccountId),
		DataSourceId: aws.String(dataSourceId),
	}

	var resp *quicksight.DescribeDataSourceOutput
	err = resource.Retry(5*time.Minute, func() *resource.RetryError {
		var err error
		resp, err := conn.DescribeDataSource(descOpts)

		if resp != nil && resp.DataSource != nil {
			status := aws.StringValue(resp.DataSource.Status)

			if status == quicksight.ResourceStatusCreationInProgress || status == quicksight.ResourceStatusUpdateInProgress {
				return resource.RetryableError(fmt.Errorf("Data Source operation still in progress (%s): %s", d.Id(), status))
			}
			if status == quicksight.ResourceStatusCreationFailed || status == quicksight.ResourceStatusUpdateFailed {
				return resource.NonRetryableError(fmt.Errorf("Data Source operation failed (%s): %s", d.Id(), status))
			}
		}

		if err != nil {
			return resource.NonRetryableError(err)
		}

		return nil
	})

	if isAWSErr(err, quicksight.ErrCodeResourceNotFoundException, "") {
		log.Printf("[WARN] QuickSight Data Source %s is already gone", d.Id())
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error describing QuickSight Data Source (%s): %s", d.Id(), err)
	}

	d.Set("arn", resp.DataSource.Arn)
	d.Set("id", resp.DataSource.DataSourceId)
	d.Set("aws_account_id", awsAccountId)

	if resp.DataSource.DataSourceParameters.AmazonElasticsearchParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"amazon_elasticsearch": {
				"domain": resp.DataSource.DataSourceParameters.AmazonElasticsearchParameters.Domain,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.AthenaParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"athena": {
				"work_group": resp.DataSource.DataSourceParameters.AthenaParameters.WorkGroup,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.AuroraParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"aurora": {
				"database": resp.DataSource.DataSourceParameters.AuroraParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.AuroraParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.AuroraParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.AuroraPostgreSqlParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"aurora_postgre_sql": {
				"database": resp.DataSource.DataSourceParameters.AuroraPostgreSqlParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.AuroraPostgreSqlParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.AuroraPostgreSqlParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.AwsIotAnalyticsParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"aws_iot_analytics": {
				"data_set_name": resp.DataSource.DataSourceParameters.AwsIotAnalyticsParameters.DataSetName,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.JiraParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"jira": {
				"site_base_url": resp.DataSource.DataSourceParameters.JiraParameters.SiteBaseUrl,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.MariaDbParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"maria_db": {
				"database": resp.DataSource.DataSourceParameters.MariaDbParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.MariaDbParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.MariaDbParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.MySqlParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"mysql": {
				"database": resp.DataSource.DataSourceParameters.MySqlParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.MySqlParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.MySqlParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.PostgreSqlParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"postgresql": {
				"database": resp.DataSource.DataSourceParameters.PostgreSqlParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.PostgreSqlParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.PostgreSqlParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.PrestoParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"presto": {
				"catalog": resp.DataSource.DataSourceParameters.PrestoParameters.Catalog,
				"host":    resp.DataSource.DataSourceParameters.PrestoParameters.Host,
				"port":    resp.DataSource.DataSourceParameters.PrestoParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.RedshiftParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"redshift": {
				"cluster_id": resp.DataSource.DataSourceParameters.RedshiftParameters.ClusterId,
				"database":   resp.DataSource.DataSourceParameters.RedshiftParameters.Database,
				"host":       resp.DataSource.DataSourceParameters.RedshiftParameters.Host,
				"port":       resp.DataSource.DataSourceParameters.RedshiftParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.S3Parameters != nil {
		d.Set("parameters", map[string]map[string]map[string]interface{}{
			"s3": {
				"manifest_file_location": {
					"bucket": resp.DataSource.DataSourceParameters.S3Parameters.ManifestFileLocation.Bucket,
					"key":    resp.DataSource.DataSourceParameters.S3Parameters.ManifestFileLocation.Key,
				},
			},
		})
	}

	if resp.DataSource.DataSourceParameters.ServiceNowParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"service_now": {
				"site_base_url": resp.DataSource.DataSourceParameters.ServiceNowParameters.SiteBaseUrl,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.SnowflakeParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"snowflake": {
				"database":  resp.DataSource.DataSourceParameters.SnowflakeParameters.Database,
				"host":      resp.DataSource.DataSourceParameters.SnowflakeParameters.Host,
				"warehouse": resp.DataSource.DataSourceParameters.SnowflakeParameters.Warehouse,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.SparkParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"spark": {
				"host": resp.DataSource.DataSourceParameters.SparkParameters.Host,
				"port": resp.DataSource.DataSourceParameters.SparkParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.SqlServerParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"sql_server": {
				"database": resp.DataSource.DataSourceParameters.SqlServerParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.SqlServerParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.SqlServerParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.TeradataParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"teradata": {
				"database": resp.DataSource.DataSourceParameters.TeradataParameters.Database,
				"host":     resp.DataSource.DataSourceParameters.TeradataParameters.Host,
				"port":     resp.DataSource.DataSourceParameters.TeradataParameters.Port,
			},
		})
	}

	if resp.DataSource.DataSourceParameters.TwitterParameters != nil {
		d.Set("parameters", map[string]map[string]interface{}{
			"twitter": {
				"max_rows": resp.DataSource.DataSourceParameters.TwitterParameters.MaxRows,
				"query":    resp.DataSource.DataSourceParameters.TwitterParameters.Query,
			},
		})
	}

	return nil
}

func resourceAwsQuickSightDataSourceUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId, dataSourceId, err := resourceAwsQuickSightDataSourceParseID(d.Id())
	if err != nil {
		return err
	}

	params := &quicksight.UpdateDataSourceInput{
		AwsAccountId: aws.String(awsAccountId),
		DataSourceId: aws.String(dataSourceId),
	}

	if credentials := resourceAwsQuickSightDataSourceCredentials(d); credentials != nil {
		params.Credentials = credentials
	}

	if dataSourceType, dataSourceParameters := resourceAwsQuickSightDataSourceParameters(d); dataSourceParameters != nil {
		params.DataSourceParameters = dataSourceParameters
		d.Set("type", dataSourceType)
	}

	// TODO: Handle permissions

	if sslProperties := resourceAwsQuickSightDataSourceSslProperties(d); sslProperties != nil {
		params.SslProperties = sslProperties
	}

	if d.HasChange("tags") {
		oraw, nraw := d.GetChange("tags")
		o := oraw.(map[string]interface{})
		n := nraw.(map[string]interface{})
		c, r := diffTagsQuickSight(tagsFromMapQuickSight(o), tagsFromMapQuickSight(n))

		if len(r) > 0 {
			_, err := conn.UntagResource(&quicksight.UntagResourceInput{
				ResourceArn: aws.String(quicksightDataSourceArn(meta.(*AWSClient).region, awsAccountId, dataSourceId)),
				TagKeys:     tagKeysQuickSight(r),
			})
			if err != nil {
				return fmt.Errorf("Error deleting QuickSight Data Source tags: %s", err)
			}
		}

		if len(c) > 0 {
			_, err := conn.TagResource(&quicksight.TagResourceInput{
				ResourceArn: aws.String(quicksightDataSourceArn(meta.(*AWSClient).region, awsAccountId, dataSourceId)),
				Tags:        c,
			})
			if err != nil {
				return fmt.Errorf("Error updating QuickSight Data Source tags: %s", err)
			}
		}
	}

	if vpcConnectionProperties := resourceAwsQuickSightDataSourceVpcConnectionProperties(d); vpcConnectionProperties != nil {
		params.VpcConnectionProperties = vpcConnectionProperties
	}

	_, err = conn.UpdateDataSource(params)
	if isAWSErr(err, quicksight.ErrCodeResourceNotFoundException, "") {
		log.Printf("[WARN] QuickSight Data Source %s is already gone", d.Id())
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error updating QuickSight Data Source %s: %s", d.Id(), err)
	}

	return resourceAwsQuickSightDataSourceRead(d, meta)
}

func resourceAwsQuickSightDataSourceDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId, dataSourceId, err := resourceAwsQuickSightDataSourceParseID(d.Id())
	if err != nil {
		return err
	}

	deleteOpts := &quicksight.DeleteDataSourceInput{
		AwsAccountId: aws.String(awsAccountId),
		DataSourceId: aws.String(dataSourceId),
	}

	if _, err := conn.DeleteDataSource(deleteOpts); err != nil {
		if isAWSErr(err, quicksight.ErrCodeResourceNotFoundException, "") {
			return nil
		}
		return fmt.Errorf("Error deleting QuickSight Data Source %s: %s", d.Id(), err)
	}

	return nil
}

func resourceAwsQuickSightDataSourceCredentials(d *schema.ResourceData) *quicksight.DataSourceCredentials {
	if v := d.Get("credentials"); v != nil {
		for _, v := range v.(*schema.Set).List() {
			credentials := v.(map[string]interface{})

			if v := credentials["credential_pair"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					credentialPairResource := v.(map[string]interface{})
					credentialPair := &quicksight.CredentialPair{}

					if v, ok := credentialPairResource["username"]; ok && v.(string) != "" {
						credentialPair.Username = aws.String(v.(string))
					}

					if v, ok := credentialPairResource["password"]; ok && v.(string) != "" {
						credentialPair.Password = aws.String(v.(string))
					}

					return &quicksight.DataSourceCredentials{
						CredentialPair: credentialPair,
					}
				}
			}
		}
	}

	return nil
}

func resourceAwsQuickSightDataSourceParameters(d *schema.ResourceData) (*string, *quicksight.DataSourceParameters) {
	if v := d.Get("parameters"); v != nil {
		for _, v := range v.(*schema.Set).List() {
			dataSourceParams := v.(map[string]interface{})
			dataSourceParamsResource := &quicksight.DataSourceParameters{}
			dataSourceType := ""

			if v := dataSourceParams["amazon_elasticsearch"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeAmazonElasticsearch
					dataSourceParamsResource.AmazonElasticsearchParameters = &quicksight.AmazonElasticsearchParameters{
						Domain: aws.String(psResource["domain"].(string)),
					}
				}
			}

			if v := dataSourceParams["athena"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					ps := &quicksight.AthenaParameters{}

					if v, ok := psResource["work_group"]; ok && v.(string) != "" {
						ps.WorkGroup = aws.String(v.(string))
					}

					dataSourceType = quicksight.DataSourceTypeAthena
					dataSourceParamsResource.AthenaParameters = ps
				}
			}

			if v := dataSourceParams["aurora"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeAurora
					dataSourceParamsResource.AuroraParameters = &quicksight.AuroraParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["aurora_postgre_sql"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeAuroraPostgresql
					dataSourceParamsResource.AuroraPostgreSqlParameters = &quicksight.AuroraPostgreSqlParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["aws_iot_analytics"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeAwsIotAnalytics
					dataSourceParamsResource.AwsIotAnalyticsParameters = &quicksight.AwsIotAnalyticsParameters{
						DataSetName: aws.String(psResource["data_set_name"].(string)),
					}
				}
			}

			if v := dataSourceParams["jira"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})

					dataSourceType = quicksight.DataSourceTypeJira
					dataSourceParamsResource.JiraParameters = &quicksight.JiraParameters{
						SiteBaseUrl: aws.String(psResource["site_base_url"].(string)),
					}
				}
			}

			if v := dataSourceParams["maria_db"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})

					dataSourceType = quicksight.DataSourceTypeMariadb
					dataSourceParamsResource.MariaDbParameters = &quicksight.MariaDbParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["mysql"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})

					dataSourceType = quicksight.DataSourceTypeMysql
					dataSourceParamsResource.MySqlParameters = &quicksight.MySqlParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["postgresql"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypePostgresql
					dataSourceParamsResource.PostgreSqlParameters = &quicksight.PostgreSqlParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["presto"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypePresto
					dataSourceParamsResource.PrestoParameters = &quicksight.PrestoParameters{
						Catalog: aws.String(psResource["catalog"].(string)),
						Host:    aws.String(psResource["host"].(string)),
						Port:    aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["redshift"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					ps := &quicksight.RedshiftParameters{
						Database: aws.String(psResource["database"].(string)),
					}

					if v, ok := psResource["cluster_id"]; ok && v.(string) != "" {
						ps.ClusterId = aws.String(v.(string))
					}

					if v, ok := psResource["host"]; ok && v.(string) != "" {
						ps.Host = aws.String(v.(string))
					}

					if v, ok := psResource["port"]; ok && v.(int64) != 0 {
						ps.Port = aws.Int64(v.(int64))
					}

					dataSourceType = quicksight.DataSourceTypeRedshift
					dataSourceParamsResource.RedshiftParameters = ps
				}
			}

			if v := dataSourceParams["s3"]; v != nil && v.(*schema.Set) != nil {
				s3 := v.(map[string]interface{})

				if v := s3["manifest_file_location"]; v != nil && v.(*schema.Set) != nil {
					for _, v := range (v.(*schema.Set)).List() {
						psResource := v.(map[string]interface{})
						dataSourceType = quicksight.DataSourceTypeS3
						dataSourceParamsResource.S3Parameters = &quicksight.S3Parameters{
							ManifestFileLocation: &quicksight.ManifestFileLocation{
								Bucket: aws.String(psResource["bucket"].(string)),
								Key:    aws.String(psResource["key"].(string)),
							},
						}
					}
				}
			}

			if v := dataSourceParams["service_now"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeServicenow
					dataSourceParamsResource.ServiceNowParameters = &quicksight.ServiceNowParameters{
						SiteBaseUrl: aws.String(psResource["site_base_url"].(string)),
					}
				}
			}

			if v := dataSourceParams["snowflake"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeSnowflake
					dataSourceParamsResource.SnowflakeParameters = &quicksight.SnowflakeParameters{
						Database:  aws.String(psResource["database"].(string)),
						Host:      aws.String(psResource["host"].(string)),
						Warehouse: aws.String(psResource["warehouse"].(string)),
					}
				}
			}

			if v := dataSourceParams["spark"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeSpark
					dataSourceParamsResource.SparkParameters = &quicksight.SparkParameters{
						Host: aws.String(psResource["host"].(string)),
						Port: aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["sql_server"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeSqlserver
					dataSourceParamsResource.SqlServerParameters = &quicksight.SqlServerParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["teradata"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeTeradata
					dataSourceParamsResource.TeradataParameters = &quicksight.TeradataParameters{
						Database: aws.String(psResource["database"].(string)),
						Host:     aws.String(psResource["host"].(string)),
						Port:     aws.Int64(psResource["port"].(int64)),
					}
				}
			}

			if v := dataSourceParams["twitter"]; v != nil && v.(*schema.Set) != nil {
				for _, v := range (v.(*schema.Set)).List() {
					psResource := v.(map[string]interface{})
					dataSourceType = quicksight.DataSourceTypeTwitter
					dataSourceParamsResource.TwitterParameters = &quicksight.TwitterParameters{
						MaxRows: aws.Int64(psResource["max_rows"].(int64)),
						Query:   aws.String(psResource["query"].(string)),
					}
				}
			}

			return aws.String(dataSourceType), dataSourceParamsResource
		}
	}

	return aws.String(""), nil
}

func resourceAwsQuickSightDataSourceSslProperties(d *schema.ResourceData) *quicksight.SslProperties {
	if v := d.Get("ssl_properties"); v != nil {
		for _, v := range v.(*schema.Set).List() {
			sslProperties := v.(map[string]interface{})

			if v, present := sslProperties["disable_ssl"]; present {
				return &quicksight.SslProperties{
					DisableSsl: aws.Bool(v.(bool)),
				}
			}
		}
	}

	return nil
}

func resourceAwsQuickSightDataSourceVpcConnectionProperties(d *schema.ResourceData) *quicksight.VpcConnectionProperties {
	if v := d.Get("vpc_connection_properties"); v != nil {
		for _, v := range v.(*schema.Set).List() {
			vpcConnectionProperties := v.(map[string]interface{})

			if v := vpcConnectionProperties["vpc_connection_arn"]; v != nil && v.(string) != "" {
				return &quicksight.VpcConnectionProperties{
					VpcConnectionArn: aws.String(v.(string)),
				}
			}
		}
	}

	return nil
}

func resourceAwsQuickSightDataSourceParseID(id string) (string, string, error) {
	parts := strings.SplitN(id, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("unexpected format of ID (%s), expected AWS_ACCOUNT_ID/DATA_SOURCE_ID", id)
	}
	return parts[0], parts[1], nil
}

func quicksightDataSourceArn(awsRegion string, awsAccountId string, dataSourceId string) string {
	return fmt.Sprintf("arn:aws:quicksight:%s:%s:datasource/%s", awsRegion, awsAccountId, dataSourceId)
}
