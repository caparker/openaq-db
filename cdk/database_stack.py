from aws_cdk import (
    aws_ec2 as _ec2,
    Stack,
    CfnOutput,
)

from constructs import Construct
import os


class DatabaseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        codeDirectory: str,
        envPath: str,
        elasticIpAllocationId: str = None,
        keyName: str = None,
        expose5432: bool = True,
        httpIpRange: str = '0.0.0.0/0',
        sshIpRange: str = None,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        # create vpc
        vpc = _ec2.Vpc(
            self,
            f"{id}-dbstack-vpc",
            cidr="10.0.0.0/16",
            nat_gateways=1,
        )

        # add some security groups
        sg = _ec2.SecurityGroup(
            self,
            f"{id}-dbstack-ssh-sg",
            vpc=vpc,
            allow_all_outbound=True,
        )

        # add an ingress rule for ssh purposes
        if sshIpRange is not None:
            sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(sshIpRange),
                connection=_ec2.Port.tcp(22)
            )

        # if we want to expose 5432 on the instance
        if expose5432:
            sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(httpIpRange),
                connection=_ec2.Port.tcp(5432)
            )

        # update and install everything that is needed for docker
        UserData = _ec2.UserData.for_linux()
        UserData.add_commands("yum update -y")
        UserData.add_commands("yum install docker -y")
        UserData.add_commands("service docker start")
        UserData.add_commands("usermod -a -G docker ec2-user")
        UserData.add_commands("chkconfig docker on")

        docker_dir = os.path.join(codeDirectory, 'docker')
        setup_dir = os.path.join(codeDirectory, 'openaqdb')

        # create the instance
        ec2 = _ec2.Instance(
            self,
            f"{id}-dbstack-database",
            instance_name=f"{id}-dbstack-database",
            instance_type=_ec2.InstanceType("t2.micro"),
            machine_image=_ec2.MachineImage.latest_amazon_linux(),
            init=_ec2.CloudFormationInit.from_elements(
                # Add some files and then build and run the docker image
                _ec2.InitFile.from_asset(
                    "/app/Dockerfile",
                    os.path.join(docker_dir, 'Dockerfile')
                ),
                # env data to use for the docker container
                _ec2.InitFile.from_asset("/app/env", envPath),
                # Because of all the subdirectories its easier just
                # to copy everything and unzip it later
                _ec2.InitFile.from_asset("/app/db.zip", setup_dir),
                # Once we copy the files over we need to
                # build and start the instance
                # the initfile method does not copy over
                # the permissions by default so
                # we need to make the init file executable
                _ec2.InitCommand.shell_command(
                     'cd /app && unzip db.zip -d openaqdb && docker build -t db-instance . && docker run --name db-openaq --env-file env --publish 5432:5432 -idt db-instance'
                ),
            ),
            vpc=vpc,
            security_group=sg,
            key_name=keyName,
            vpc_subnets=_ec2.SubnetSelection(
                subnet_type=_ec2.SubnetType.PUBLIC
            ),
            user_data=UserData
        )

        # if we want to assign a specific ip address
        # this can be handy for staging but probably where you
        # may be destroying and rebuilding a lot but
        # not worth it for production
        # where something will be deployed and left alone
        if elasticIpAllocationId is not None:
            _ec2.CfnEIPAssociation(
                self,
                f"{id}-dbstack-ipaddress",
                allocation_id=elasticIpAllocationId,
                instance_id=ec2.instance_id,
            )

        CfnOutput(
            scope=self,
            id=f"{id}-public-ip",
            value=ec2.instance_public_ip,
            description="public ip",
            export_name=f"{id}-public-ip")
