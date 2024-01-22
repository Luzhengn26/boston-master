#!groovy
@Library('utils') import de.tech26.Utils
def utils = new Utils(steps, env, scm, currentBuild)

node {
  try {
    utils.configureBuild()

    withCredentials([string(credentialsId: 'jenkins-artifactory-pip-url', variable: 'N26_PIP_INDEX_URL')]) {
      stage("Check Black Linting") {
        sh "make format"
      }
      utils.dockerBuild()
    }

    if (env.BRANCH_NAME == "master") {
        lock("${env.JOB_NAME}") {

            stage("Push to S3") {
                sshagent([utils.credentials]) {
                    sh "aws s3 rm s3://research-internal.tech26.de/"
                    sh "aws s3 sync src/setup/frontend/static_copy/  s3://research-internal.tech26.de/"
                }
            }
        }
    } else {
        echo "Branch ${env.BRANCH_NAME} is not deployable. Skipping remaining pipeline."
    }

    currentBuild.result = "SUCCESS"
  } catch (err) {
    echo "ERROR: ${err}"

    if (currentBuild.result == null) {
        currentBuild.result = "FAILURE"
    }
  } finally {
    utils.sendNotification()
  }
}
