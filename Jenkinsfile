pipeline {
    agent { label 'ec2orchestrator' }

    environment {
        // Repository parameters
        odahuGithubCredsID = "63e5abfa-a815-4671-937f-7e00b1e45725"
        // GCP parameters
        gcpProjectID = "or2-msq-epmd-legn-t1iylu"
        gcpLocation = "us-east1"
        gcpCloudCredsID = "gcp-epmd-legn-legion-automation"

        uploadBaseDataset = "${params.uploadBaseDataset}"
    }

    stages {
        stage('Checkout') {
            steps {
                cleanWs()
                script {
                    currentBuild.description = "${ComposerName} ${params.GitBranch}"
                    sshagent([odahuGithubCredsID]) {
                        sh """#!/bin/bash -ex
                               #TODO get repo url from passed parameters
                               mkdir -p \$(getent passwd \$(whoami) | cut -d: -f6)/.ssh && ssh-keyscan github.com >> \$(getent passwd \$(whoami) | cut -d: -f6)/.ssh/known_hosts
                               git clone ${params.GitRepo} ./
                               git checkout ${params.GitBranch}
                           """
                    }
                }
            }
        }

        stage('Upload base dataset') {
            when {
                expression { return env.uploadBaseDataset.toBoolean() }
            }
            steps {
                script {
                    dir('data/base_dataset'){
                        sh "tar -czvf base_dataset.tar.gz *"
                    }
                    dir('data/realtime_dataset'){
                        sh "tar -czvf realtime_dataset.tar.gz *"
                    }
                    withCredentials([file(credentialsId: gcpCloudCredsID, variable: 'gcpCredentials')]) {
                        docker.image("google/cloud-sdk:alpine").inside("-e GOOGLE_CREDENTIALS=${gcpCredentials} -u root") {
                            sh "gcloud auth activate-service-account --key-file=${gcpCredentials} --project=${gcpProjectID}"
                            sh "gsutil cp data/base_dataset/base_dataset.tar.gz ${Bucket}/data/"
                            sh "gsutil cp data/realtime_dataset/realtime_dataset.tar.gz ${Bucket}/data/"
                        }
                    }
                }
            }
        }

        stage('Upload dags') {
            steps {
                script {
                    withCredentials([file(credentialsId: gcpCloudCredsID, variable: 'gcpCredentials')]) {
                        docker.image("google/cloud-sdk:alpine").inside("-e GOOGLE_CREDENTIALS=${gcpCredentials} -u root") {
                            sh "gcloud auth activate-service-account --key-file=${gcpCredentials} --project=${gcpProjectID}"
                            def dagGCSPrefix = sh(script: "gcloud composer environments describe ${ComposerName} --location ${gcpLocation} --format='get(config.dagGcsPrefix)'", returnStdout: true)
                            zip zipFile: 'dags/reuters.zip', archive: false, dir: 'pipeline'
                            sh "gsutil -m cp -r manifests ${dagGCSPrefix}"
                            sh "gsutil -m cp -r dags ${dagGCSPrefix}"
                        }
                    }
                }
            }
        }
    }

    post {
        cleanup {
            deleteDir()
        }
    }
}
