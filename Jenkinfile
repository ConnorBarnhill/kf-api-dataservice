pipeline {
  triggers {
        pollSCM('* * * * *')
  }
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
        slackSend (color: '#ddaa00', message: ":construction_worker: kf-api-dataservice GETTING SCRIPTS:")
        sh "git clone -b bugfix/autoscaling git@github.com:kids-first/kf-api-dataservice-config.git"
      }
    }
    stage('Test') {
     steps {
       slackSend (color: '#ddaa00', message: ":construction_worker: kf-api-dataservice TESTING STARTED: (${env.BUILD_URL})")
       sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/test_stage/test.sh"
       slackSend (color: '#41aa58', message: ":white_check_mark: kf-api-dataservice TESTING COMPLETED: (${env.BUILD_URL})")
     }
     post {
       failure {
         slackSend (color: '#ff0000', message: ":frowning: kf-api-dataservice Test Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
       }
     }
    }
    stage('Build') {
      steps {
        sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/build_stage/build.sh"
      }
    }
    stage('Publish') {
      steps {
        sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/publish_stage/publish.sh"
        slackSend (color: '#41aa58', message: ":arrow_up: kf-api-dataservice PUSHED IMAGE: (${env.BUILD_URL})")
      }
      post {
        failure {
          slackSend (color: '#ff0000', message: ":frowning: kf-api-dataservice Publish Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage('Deploy Dev') {
      when {
        expression {
          return env.BRANCH_NAME != 'master' & tag == '' ;
        }
      }
      steps {
        slackSend (color: '#005e99', message: ":deploying_dev: DEPLOYING TO DEVELOPMENT: (${env.BUILD_URL})")
        sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/deploy_stage/deploy.sh dev"
        sh "cleanup"
        slackSend (color: '#41aa58', message: ":white_check_mark: kf-api-dataservice DEPLOYED TO DEVELOPMENT: (${env.BUILD_URL})")
      }
      post {
        failure {
          slackSend (color: '#ff0000', message: ":frowning: kf-api-dataservice Deploy Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage('Retag with pre-release'){
      when {
        expression {
           return env.BRANCH_NAME == 'master' & tag != '';
        }
        expression {
          return tag != '';
        }
      }
      steps {
        slackSend (color: '#005e99', message: ":deploying_qa: Retagging image with 'pre-release'")
        sh "MANIFEST=\$(aws ecr batch-get-image --region us-east-1 --repository-name kf-api-dataservice --image-ids imageTag=latest --query images[].imageManifest --output text)"
        sh "aws ecr put-image --region us-east-1 --repository-name kf-api-dataservice --image-tag \"prerelease-$tag\" --image-manifest \"$MANIFEST\""
      }
    }
    stage('Deploy QA') {
      when {
       expression {
            return env.BRANCH_NAME == 'master' || ( tag != '' & env.BRANCH_NAME == tag);
       }
     }
     steps {
       slackSend (color: '#005e99', message: ":deploying_qa: kf-api-dataservice DEPLOYING TO QA: (${env.BUILD_URL})")
       sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/deploy_stage/deploy.sh qa"
       sh "cleanup"
       slackSend (color: '#41aa58', message: ":white_check_mark: kf-api-dataservice DEPLOYED TO QA: (${env.BUILD_URL})")
     }
     post {
       failure {
         slackSend (color: '#ff0000', message: ":frowning: kf-api-dataservice Deploy Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
       }
     }
    }
    stage('Promotion to PRD') {
      when {
             expression {
               return env.BRANCH_NAME == 'master' || ( tag != '' & env.BRANCH_NAME == tag);
             }
             expression {
               return tag != '';
             }
           }
      steps {
             script {
                     env.DEPLOY_TO_PRD = input message: 'User input required',
                                     submitter: 'lubneuskia,heatha,eubankj,vermar,andricd',
                                     parameters: [choice(name: 'Deploy to PRD Environment', choices: 'no\nyes', description: 'Choose "yes" if you want to deploy the PRD server')]
             }
     }
    }
    stage('Retag with release'){
      when {
        environment name: 'DEPLOY_TO_PRD', value: 'yes'
        expression {
            return env.BRANCH_NAME == 'master' & tag != '';
        }
        expression {
          return tag != '';
        }
      }
      steps {
        slackSend (color: '#005e99', message: ":deploying_prd: Retagging image with 'release'")
        sh "MANIFEST=\$(aws ecr batch-get-image --region us-east-1 --repository-name kf-api-dataservice --image-ids imageTag=\"prerelease-$tag\" --query images[].imageManifest --output text)"
        sh "aws ecr put-image --region us-east-1 --repository-name kf-api-dataservice --image-tag \"$tag\" --image-manifest \"$MANIFEST\""
      }
      post {
        failure {
          slackSend (color: '#ff0000', message: ":frowning: kf-api-dataservice Deploy Failed: Branch '${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
      }
    }
    stage('Deploy PRD') {
      when {
       environment name: 'DEPLOY_TO_PRD', value: 'yes'
       expression {
            return env.BRANCH_NAME == 'master' || ( tag != '' & env.BRANCH_NAME == tag);
       }
       expression {
         return tag != '';
       }
     }
     steps {
       slackSend (color: '#005e99', message: ":deploying_prd: kf-api-dataservice DEPLOYING TO PRD: (${env.BUILD_URL})")
       sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/deploy_stage/deploy.sh prd"
       sh "cleanup"
       slackSend (color: '#41aa58', message: ":white_check_mark: kf-api-dataservice DEPLOYED TO PRD: (${env.BUILD_URL})")
     }
    }
    stage("Rollback to previous version of the application with DB Rollback") {
      when {
             expression {
                  return env.BRANCH_NAME == 'master' || ( tag != '' & env.BRANCH_NAME == tag);
             }
             expression {
               return tag != '';
             }
           }
      steps {
             script {
                     env.ROLLBACK_PRD = input message: 'User input required',
                                     submitter: 'lubneuskia,heatha,eubankj,vermar,andricd',
                                     parameters: [choice(name: 'Rollback PRD to Previous Version?', choices: 'noway\nrollback', description: 'Choose "yes" if you want to rollback the PRD deployment to previous stable release')]
             }
     }
    }
    stage('Rollback PRD') {
      when {
       environment name: 'ROLLBACK_PRD', value: 'rollback'
       expression {
           return env.BRANCH_NAME == 'master' || ( tag != '' & env.BRANCH_NAME == tag);
       }
       expression {
         return tag != '';
       }
     }
     steps {
       slackSend (color: '#005e99', message: ":deploying_prd: kf-api-dataservice DEPLOYING TO PRD: (${env.BUILD_URL})")
       sh "kf-api-dataservice-config/aws-ecs-service-type-1/ci-scripts/rollback/rollback.sh"
       sh "cleanup"
       slackSend (color: '#41aa58', message: ":white_check_mark: kf-api-dataservice DEPLOYED TO PRD: (${env.BUILD_URL})")
     }
    }
  }
}
