#!groovy
properties([
    pipelineTriggers([[$class:"GitHubPushTrigger"]])
])
pipeline {
  agent { label 'docker-slave' }
  stages{
    stage('Get Code') {
      steps {
          deleteDir()
          checkout ([
              $class: 'GitSCM',
              branches: scm.branches,
              doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
              extensions: [[$class: 'CloneOption', noTags: false, shallow: false, depth: 0, reference: '']],
              userRemoteConfigs: scm.userRemoteConfigs,
           ])
           script {
               tag=sh(returnStdout: true, script: "git tag -l --points-at HEAD").trim()
               env.tag = tag
             }
      }
    }
    stage('GetOpsScripts') {
      steps {
        slackSend (color: '#ddaa00', message: ":construction_worker: GETTING SCRIPTS:")
        sh '''
        git clone git@github.com:kids-first/kf-api-dataservice-config.git
        '''
      }
    }
    stage('Test') {
     steps {
       slackSend (color: '#ddaa00', message: ":construction_worker: TESTING STARTED: (${env.BUILD_URL})")
       sh '''
       kf-api-dataservice-config/ci-scripts/test_stage/test.sh
       '''
       slackSend (color: '#41aa58', message: ":white_check_mark: TESTING COMPLETED: (${env.BUILD_URL})")
     }
     post {
       failure {
         slackSend (color: '#ff0000', message: ":frowning: Test Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
       }
     }
    }
    stage('Build') {
      steps {
        sh '''
        kf-api-dataservice-config/ci-scripts/build_stage/build.sh
        '''
      }
    }
    stage('Publish') {
      steps {
        sh '''
        kf-api-dataservice-config/ci-scripts/publish_stage/publish.sh
        '''
        slackSend (color: '#41aa58', message: ":arrow_up: PUSHED IMAGE: (${env.BUILD_URL})")
      }
      post {
        failure {
          slackSend (color: '#ff0000', message: ":frowning: Publish Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage('Deploy Dev') {
      when {
        expression {
          return env.BRANCH_NAME != 'master';
        }
      }
      steps {
        slackSend (color: '#005e99', message: ":deploying_dev: DEPLOYING TO DEVELOPMENT: (${env.BUILD_URL})")
        sh '''
        aws rds create-db-snapshot --db-instance-identifier kf-dataservice-api-dev --db-snapshot-identifier kf-dataservice-api-dev-$GIT_COMMIT-$BUILD_NUMBER --region us-east-1
        kf-api-dataservice-config/ci-scripts/deploy_stage/deploy.sh dev
        '''
        slackSend (color: '#41aa58', message: ":white_check_mark: DEPLOYED TO DEVELOPMENT: (${env.BUILD_URL})")
      }
      post {
        failure {
          slackSend (color: '#ff0000', message: ":frowning: Test Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage("Rollback dataservice-api ?") {
      steps {
             script {
                     env.ROLL_BACK = input message: 'User input required',
                                     submitter: 'lubneuskia,heatha',
                                     parameters: [choice(name: 'dataservice-api: Deploy to PRD Environment', choices: 'no\nyes', description: 'Choose "yes" if you want to deploy the PRD server')]
             }
     }
    }
    stage('Rollback PRD') {
      when {
       environment name: 'ROLL_BACK', value: 'yes'
       expression {
           return env.BRANCH_NAME != 'master';
       }
     }
     steps {
       sh '''
       kf-api-dataservice-config/ci-scripts/rollback/rollback.sh dev 
       '''
     }
    }
  }
}
