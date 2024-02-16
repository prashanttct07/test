#!/usr/bin/bash
################################################################################
#                              scriptTemplate                                  #
#                                                                              #
# Use this template to generate dummy data simulating IOT sensors              #
#                                                                              #
# Change History                                                               #
# 04/06/2023  Prashant Agrawal                                                 #
################################################################################


################################################################################
# Help                                                                         #
################################################################################
Help()
{
   # Display Help
   echo
   echo "Usage:"
   echo
   echo "b -> bulk_size; default -> 7500; As this program generally generates 7500 events per sec"
   echo "c -> Number of parallel generator to run; defaults to 1"
   echo "e -> Amazon OpenSearch endpoint; Service|Serverless; Setup -s to define es vs aoss"
   echo "i -> index_rotation; 'single' or 'daily'; Default is daily Index, means all data will be sent to daily created index"
   echo "r -> Amazon OpenSearch Service/Serverless Region"
   echo "s -> To define if this is 'es' or 'aoss' as service; defaults to 'aoss'"
   echo
   echo "Sample: bash replicate_data_simulator.sh -b 7500 -i single -r us-west-2 -e endpoint.us-west-2.aoss.amazonaws.com "
}
#python3 opensearch-client-queries.py -r us-east-1 -e eqozmxpkm9amcwhakbcj.us-east-1.aoss.amazonaws.com &
#bash replicate_data_simulator.sh -b 7500 -i daily -r us-east-1 -e eqozmxpkm9amcwhakbcj.us-east-1.aoss.amazonaws.com -c 1


################################################################################
# Main program                                                                 #
################################################################################
# Get the options
c=1
s='aoss'
b=7500
i='daily'
e=''
r=''

while getopts "b:c:e:h:i:r:s:" option; do
   case $option in
      b) b=${OPTARG};;
      c) c=${OPTARG};;
      e) e=${OPTARG};;
      h) Help
         exit;;
      i) i=${OPTARG};;
      r) r=${OPTARG};;
      s) s=${OPTARG};;
     \?) # incorrect option
         echo "Error: Invalid option"
         Help
         exit;;
   esac
done

if [ -z "$e" ]
then
   echo
   echo "Error: Please provide OpenSearch Service/Serverless endpoint"
   echo "example: -e <endpoint>"
   echo
   echo "------------------------------------------------------------"
   Help
   exit
fi

if [ -z "$r" ]
then
   echo
   echo "Error: Please provide OpenSearch Service/Serverless region"
   echo "example: -r <region>"
   echo
   echo "------------------------------------------------------------"
   Help
   exit
fi


kill -9 $(ps -ef | grep -i "opensearch-client-generator_" | awk '{print $2}')
rm -rf opensearch-client-generator_*

# Run $c occurrence of python job to add multiple request at once
for ((x=1; x <= $c; x++))
do
  cp opensearch-client-generator.py opensearch-client-generator_$x.py
  python3 opensearch-client-generator_$x.py -b $b -s $s -i $i -e $e -r $r &
  echo "Started instance: $x"
  sleep 10
done
