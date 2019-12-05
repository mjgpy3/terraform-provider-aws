package aws

import (
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/quicksight"
)

func resourceAwsQuickSightTemplate() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsQuickSightTemplateCreate,
		Read:   resourceAwsQuickSightTemplateRead,
		Update: resourceAwsQuickSightTemplateUpdate,
		Delete: resourceAwsQuickSightTemplateDelete,

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

			"version_arn": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"name": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.NoZeroValues,
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

			"source_entity": {
				Type:     schema.TypeList,
				Required: true,
				MaxItems: 1,
				MinItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"source_analysis": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"arn": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validateArn,
									},
									"data_set_references": {
										Type:     schema.TypeList,
										Optional: true,
										MaxItems: 1,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"arn": {
													Type:         schema.TypeString,
													Required:     true,
													ValidateFunc: validateArn,
												},
												"placeholder": {
													Type:     schema.TypeString,
													Required: true,
												},
											},
										},
									},
								},
							},
						},
						"source_template": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"arn": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validateArn,
									},
								},
							},
						},
					},
				},
			},

			"tags": tagsSchema(),

			"template_id": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew: true,
				ValidateFunc: validation.NoZeroValues,
			},

			"version_description": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.NoZeroValues,
			},
		},
	}
}

func resourceAwsQuickSightTemplateCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId := meta.(*AWSClient).accountid

	if v, ok := d.GetOk("aws_account_id"); ok {
		awsAccountId = v.(string)
	}

	createOpts := &quicksight.CreateTemplateInput{
		AwsAccountId: aws.String(awsAccountId),
                TemplateId: aws.String(v.Get("template_id").(string)),
	}

	if name, ok := d.GetOk("name"); ok {
		createOpts.Name = aws.String(name)
	}

	if versionDescription, ok := d.GetOk("version_description"); ok {
		createOpts.VersionDescription = aws.String(version_description)
	}

	if v := d.Get("permission"); v != nil && len(v.([]interface{})) != 0 {
		createOpts.Permissions = make([]*quicksight.ResourcePermission, 0)

		for _, v := range v.([]interface{}) {
			permissionResource := v.(map[string]interface{})
			permission := &quicksight.ResourcePermission{
				Actions:   expandStringSet(permissionResource["actions"].(*schema.Set)),
				Principal: aws.String(permissionResource["principal"].(string)),
			}

			createOpts.Permissions = append(params.Permissions, permission)
		}
	}

	if v := d.Get("source_entity"); v != nil {
		for _, v := range v.([]interface{}) {
			sourceEntity := v.(map[string]interface{})

			if arn, dataSetReferences, found := resourceAwsQuickSightGetSourceEntity(sourceEntity, "source_analysis"); found {
				createOpts.SourceEntity = &quicksight.TemplateSourceEntity{
					SourceAnalysis: &quicksight.TemplateSourceAnalysis{
						Arn:               arn,
						DataSetReferences: dataSetReferences,
					},
				}
			}

			if arn, dataSetReferences, found := resourceAwsQuickSightGetSourceEntity(sourceEntity, "source_template"); found {
				createOpts.SourceEntity = &quicksight.TemplateSourceEntity{
					SourceTemplate: &quicksight.TemplateSourceTemplate{
						Arn:               arn,
						DataSetReferences: dataSetReferences,
					},
				}
			}

		}
	}

	if v, ok := d.GetOk("tags"); ok {
		params.Tags = tagsFromMapQuickSight(v.(map[string]interface{}))
	}

	resp, err := conn.CreateTemplate(createOpts)
	if err != nil {
		return fmt.Errorf("Error creating QuickSight Template: %s", err)
	}

	d.SetId(fmt.Sprintf("%s/%s", awsAccountId, aws.StringValue(resp.TemplateId)))

	return resourceAwsQuickSightTemplateRead(d, meta)
}

func resourceAwsQuickSightTemplateRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId, templateId, err := resourceAwsQuickSightGroupParseID(d.Id())
	if err != nil {
		return err
	}

	descOpts := &quicksight.DescribeGroupInput{
		AwsAccountId: aws.String(awsAccountId),
		Namespace:    aws.String(namespace),
		GroupName:    aws.String(groupName),
	}

	resp, err := conn.DescribeGroup(descOpts)
	if isAWSErr(err, quicksight.ErrCodeResourceNotFoundException, "") {
		log.Printf("[WARN] QuickSight Group %s is already gone", d.Id())
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error describing QuickSight Group (%s): %s", d.Id(), err)
	}

	d.Set("arn", resp.Group.Arn)
	d.Set("aws_account_id", awsAccountId)
	d.Set("group_name", resp.Group.GroupName)
	d.Set("description", resp.Group.Description)
	d.Set("namespace", namespace)

	return nil
}

func resourceAwsQuickSightGroupUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId, namespace, groupName, err := resourceAwsQuickSightGroupParseID(d.Id())
	if err != nil {
		return err
	}

	updateOpts := &quicksight.UpdateGroupInput{
		AwsAccountId: aws.String(awsAccountId),
		Namespace:    aws.String(namespace),
		GroupName:    aws.String(groupName),
	}

	if v, ok := d.GetOk("description"); ok {
		updateOpts.Description = aws.String(v.(string))
	}

	_, err = conn.UpdateGroup(updateOpts)
	if isAWSErr(err, quicksight.ErrCodeResourceNotFoundException, "") {
		log.Printf("[WARN] QuickSight Group %s is already gone", d.Id())
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error updating QuickSight Group %s: %s", d.Id(), err)
	}

	return resourceAwsQuickSightGroupRead(d, meta)
}

func resourceAwsQuickSightGroupDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountId, namespace, groupName, err := resourceAwsQuickSightTemplateParseID(d.Id())
	if err != nil {
		return err
	}

	deleteOpts := &quicksight.DeleteGroupInput{
		AwsAccountId: aws.String(awsAccountId),
		Namespace:    aws.String(namespace),
		GroupName:    aws.String(groupName),
	}

	if _, err := conn.DeleteGroup(deleteOpts); err != nil {
		if isAWSErr(err, quicksight.ErrCodeResourceNotFoundException, "") {
			return nil
		}
		return fmt.Errorf("Error deleting QuickSight Group %s: %s", d.Id(), err)
	}

	return nil
}

func resourceAwsQuickSightGetSourceEntity(sourceEntity map[string]interface{}, keyToTry string) (*string, []*quicksight.DataSetReferences, bool) {
	if v := sourceEntity[keyToTry]; v != nil && v.([]interface{}) != nil {
		dataSetReferences := make([]*quicksight.DataSetReference, 0)
		var arn *string

		for _, v := range v.([]interface{}) {
			entity := v.(map[string]interface{})
			arn = aws.String(entity["arn"].(string))

			for _, v := range entity["data_set_references"].([]interface{}).List() {
				dataSetRef := v.(map[string]interface{})

				dataSetReferences = append(dataSetReferences, &quicksight.DataSetReference{
					DataSetArn:         aws.String(dataSetRef["arn"].(string)),
					DataSetPlaceholder: aws.String(dataSetRef["placeholder"].(string)),
				})
			}
		}

		return arn, dataSetReferences, true
	}

	return aws.String(""), nil, false
}

func resourceAwsQuickSightTemplateParseID(id string) (string, string, string, error) {
	parts := strings.SplitN(id, "/", 3)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("unexpected format of ID (%s), expected AWS_ACCOUNT_ID/NAMESPACE/GROUP_NAME", id)
	}
	return parts[0], parts[1], parts[2], nil
}
