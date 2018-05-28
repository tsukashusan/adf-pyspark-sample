#Set variables one by one.
$subscriptionID = "dc5d3c89-36dd-4a3c-b09b-e6ee41f6d5b5" # Your Azure subscription ID
$resourceGroupName = "adfondemand20180523" # Name of the resource group
$dataFactoryName = "adfondemand20180523" # Globally unique name of the data factory
$pipelineName = "MyOnDemandSparkLinkedService" # Name of the pipeline
$locations = @("eastus", "eastus2", "southeastasia", "japaneast", "japanwest")

#Run the following command, and enter the user name and password that you use to sign in to the Azure portal:
Login-AzureRmAccount

#Run the following command to view all the subscriptions for this account:
#Get-AzureRmSubscription

#Run the following command to select the subscription that you want to work with. Replace SubscriptionId with the ID of your Azure subscription:
Select-AzureRmSubscription -SubscriptionId $subscriptionID

#Get-AzureRmLocation|select DisplayName, Location

#Create the resource group: ADFTutorialResourceGroup.
New-AzureRmResourceGroup -Name $resourceGroupName -Location $locations[3]

#Create the data factory.
Set-AzureRmDataFactoryV2 -ResourceGroupName $resourceGroupName -Location $locations[1] -Name $dataFactoryName

#Switch to the folder where you created JSON files, and run the following command to deploy an Azure Storage linked service:
Set-AzureRmDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $resourceGroupName -Name "MyStorageLinkedService" -File "MyStorageLinkedService.json"

#Run the following command to deploy an on-demand Spark linked service
Set-AzureRmDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $resourceGroupName -Name "MyOnDemandSparkLinkedService" -File "MyOnDemandSparkLinkedService.json"

#Run the following command to deploy a pipeline:
Set-AzureRmDataFactoryV2Pipeline -DataFactoryName $dataFactoryName -ResourceGroupName $resourceGroupName -Name $pipelineName -File "MySparkOnDemandPipeline.json"
#Start and monitor a pipeline run
#Start a pipeline run. It also captures the pipeline run ID for future monitoring.
$runId = Invoke-AzureRmDataFactoryV2Pipeline -DataFactoryName $dataFactoryName -ResourceGroupName $resourceGroupName -PipelineName $pipelineName
Start-Sleep -s 30
#Run the following script to continuously check the pipeline run status until it finishes.
while ($True) {
    $result = Get-AzureRmDataFactoryV2ActivityRun -DataFactoryName $dataFactoryName -ResourceGroupName $resourceGroupName -PipelineRunId $runId -RunStartedAfter (Get-Date).AddMinutes(-30) -RunStartedBefore (Get-Date).AddMinutes(30)

    if(!$result) {
        Write-Host "Waiting for pipeline to start..." -foregroundcolor "Yellow"
    }
    elseif (($result | Where-Object { $_.Status -eq "InProgress" } | Measure-Object).count -ne 0) {
        Write-Host "Pipeline run status: In Progress" -foregroundcolor "Yellow"
    }
    else {
        Write-Host "Pipeline '"$pipelineName"' run finished. Result:" -foregroundcolor "Yellow"
        $result
        break
    }
    ($result | Format-List | Out-String)
    Start-Sleep -Seconds 15
}

Write-Host "Activity `Output` section:" -foregroundcolor "Yellow"
$result.Output -join "`r`n"

Write-Host "Activity `Error` section:" -foregroundcolor "Yellow"
$result.Error -join "`r`n"