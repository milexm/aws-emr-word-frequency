# aws-emr-word-frequency
This example helps you getting started with AWS Elastic MapReduce (EMR).
It shows how to obtain word frequency and produce a list of 
words sorted in ascending order from the least to the most frequently used word. 
The application uses 2 steps, hence it needs two mapper functions and 2 reducer 
functions.

<h3>Running the Application</h3>
<ol>
    <li>Locally in the editor console execute this command: </br>
        <code>
            !python word_frequency_sorted.py word_frequency_book.txt > wfs.txt
        </code>
    </li>
    <li>Remotely in the AWS; in the Canopy command terminalexecute this command: </br>
        <code>
             python word_frequency_sorted.py -r emr --conf-path=C:\Users\[user name]\.mrjob.config word_frequency_book.txt > wfs.txt
        </code>
        <br/>
        Notice the directory where to store the <i>.mrjob.config</i> file can be any of your choice.
    </li>
    
</ol>

<h3>Minimal .mrjob.config Example</h3>  
The following is an example of a minimal configuration file.       
<pre>
<code>
runners:
  emr:
    ec2_key_pair: [keypairfile] # Name of your key pair file
    ec2_key_pair_file: [C:\\dir\\keypairfile.pem] # Path of your key pair file
    aws_region: us-west-2
    ec2_instance_type: m1.small
    num_ec2_instances: 2
    ssh_tunnel_to_job_tracker: true
</code>
</pre>

<h3>References</h3>
<ul>
    <li><a href="http://aws.amazon.com/elasticmapreduce/" target="_blank">Amazon EMR</a> </li>
    <li><a href="https://www.enthought.com/products/canopy/" target="_blank">Canopy</a> </li>
</ul>
