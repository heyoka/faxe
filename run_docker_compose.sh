    #!/bin/bash
    if [[ $(docker ps -a | grep faxe ) ]]; 
      then
        docker kill faxe; 
        docker container rm faxe;
        echo "running faxe found - kill+remove it"
      else
        echo "no running faxe found - skip"
    fi
    cd /opt/faxe/faxe_repo
    docker-compose -f docker-compose-devat.yml  up -d