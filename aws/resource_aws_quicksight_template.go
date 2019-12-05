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

	awsAccountID := meta.(*AWSClient).accountid
	namespace := d.Get("namespace").(string)

	if v, ok := d.GetOk("aws_account_id"); ok {
		awsAccountID = v.(string)
	}

	createOpts := &quicksight.CreateGroupInput{
		AwsAccountId: aws.String(awsAccountID),
		Namespace:    aws.String(namespace),
		GroupName:    aws.String(d.Get("group_name").(string)),
	}

	if v, ok := d.GetOk("description"); ok {
		createOpts.Description = aws.String(v.(string))
	}

	resp, err := conn.CreateGroup(createOpts)
	if err != nil {
		return fmt.Errorf("Error creating QuickSight Group: %s", err)
	}

	d.SetId(fmt.Sprintf("%s/%s/%s", awsAccountID, namespace, aws.StringValue(resp.Group.GroupName)))

	return resourceAwsQuickSightGroupRead(d, meta)
}

func resourceAwsQuickSightGroupRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountID, namespace, groupName, err := resourceAwsQuickSightGroupParseID(d.Id())
	if err != nil {
		return err
	}

	descOpts := &quicksight.DescribeGroupInput{
		AwsAccountId: aws.String(awsAccountID),
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
	d.Set("aws_account_id", awsAccountID)
	d.Set("group_name", resp.Group.GroupName)
	d.Set("description", resp.Group.Description)
	d.Set("namespace", namespace)

	return nil
}

func resourceAwsQuickSightGroupUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).quicksightconn

	awsAccountID, namespace, groupName, err := resourceAwsQuickSightGroupParseID(d.Id())
	if err != nil {
		return err
	}

	updateOpts := &quicksight.UpdateGroupInput{
		AwsAccountId: aws.String(awsAccountID),
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

	awsAccountID, namespace, groupName, err := resourceAwsQuickSightGroupParseID(d.Id())
	if err != nil {
		return err
	}

	deleteOpts := &quicksight.DeleteGroupInput{
		AwsAccountId: aws.String(awsAccountID),
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

func resourceAwsQuickSightGroupParseID(id string) (string, string, string, error) {
	parts := strings.SplitN(id, "/", 3)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("unexpected format of ID (%s), expected AWS_ACCOUNT_ID/NAMESPACE/GROUP_NAME", id)
	}
	return parts[0], parts[1], parts[2], nil
}
