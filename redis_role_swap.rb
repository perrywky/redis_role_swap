#!/usr/bin/env ruby
# 
# Redis role swap
#

require 'rubygems'
require 'redis'
require 'statemachine'
require 'colorize'
require 'choice'
require 'pp'
require 'yaml'
require 'securerandom'
require 'timeout'

PROGRAM_VERSION = "0.1"

Choice.options do

    header ''
    header 'Specific options:'

    option :config do
        short '-c'
        long '--config=PATH'
        desc 'Path to cluster.yml'
        cast String
    end

    option :check do
        long '--check'
        desc 'Just run the initial checks.'
    end

    option :version do
        short '-v'
        long '--version'
        desc 'Show version'
        action do
            puts "Redis Role Swap v#{PROGRAM_VERSION}"
            exit
        end
    end
end

CHOICES = Choice.choices

if CHOICES.empty?
    Choice.help
end

if CHOICES[:config]
    CONFIG = YAML::load(IO.read(CHOICES[:config]))
else
    puts "Please specify config file with --config=PATH"
    exit
end

FLOATING_IP = CONFIG['floating_ip']
FLOATING_IP_CIDR = CONFIG['floating_ip_cidr']
INTERFACE = CONFIG['interface']
MASTER_IPMI_ADDRESS = CONFIG['master_ipmi_address']
SSH_USER = CONFIG['ssh_user']
if CONFIG['ssh_identify_file'] then
    SSH_OPTIONS = "-i #{CONFIG['ssh_identify_file']}"
else
    SSH_OPTIONS = ""
end

class MyRedis < Redis

    attr_accessor :options

    def options
        @options
    end

    def initialize(config)
        super(config)
        @options = config
        if config['auth']
            self.auth(config['auth'])
        end
    end

    def role
        if redis_rep_role == "master" && ip_role == "master"
            "master"
        else
            "slave"
        end
    end

    def redis_rep_role
        self.info("replication")["role"]
    end

    def ip_role
        `ssh #{SSH_USER}@#{@options['host']} #{SSH_OPTIONS} 'sudo /sbin/ip addr | grep #{FLOATING_IP}#{FLOATING_IP_CIDR}'`
        if $?.exitstatus == 0
           "master"
        else
            "slave"
        end
    end

    def version
        return self.info()['redis_version']
    end

    def hostname
        `host #{@options['host']}`.split(" ").last.gsub(/.\Z/, "").split(".").first
    end

    def read_only?
        if redis_rep_role == "slave" and self.config("GET", "slave-read-only").last == "yes"
            true
        else
            false
        end
    end

    def arping_path
        `ssh #{SSH_USER}@#{self.options['host']} #{SSH_OPTIONS} 'sudo /sbin/arping -V 2> /dev/null'`
        if $?.exitstatus == 0
            return "/sbin/arping"
        end
        `ssh #{SSH_USER}@#{self.options['host']} #{SSH_OPTIONS} 'sudo /usr/bin/arping -V 2> /dev/null'`
        if $?.exitstatus == 0
            return "/usr/bin/arping"
        end
    end

    def arping
        if self.options['host'] == `hostname`.chomp
            `sudo #{self.arping_path} -U -c 4 -I #{INTERFACE} #{FLOATING_IP}`
        else
            `ssh #{SSH_USER}@#{self.options['host']} #{SSH_OPTIONS} 'sudo #{self.arping_path} -U -c 4 -I #{INTERFACE} #{FLOATING_IP}'`
        end
        if $?.exitstatus == 0
            true
        else
            false
        end
    end

    def remove_vip
        if self.options['host'] == `hostname`.chomp
            `sudo /sbin/ip addr del #{FLOATING_IP}#{FLOATING_IP_CIDR} dev #{INTERFACE}`
        else
            `ssh #{SSH_USER}@#{self.options['host']} #{SSH_OPTIONS} 'sudo /sbin/ip addr del #{FLOATING_IP}#{FLOATING_IP_CIDR} dev #{INTERFACE}'`
        end
        if $?.exitstatus == 0
            true
        else
            false
        end
    end

    def add_vip
        if self.options['host'] == `hostname`.chomp
            `sudo /sbin/ip addr add #{FLOATING_IP}#{FLOATING_IP_CIDR} dev #{INTERFACE}`
        else
            `ssh #{SSH_USER}@#{self.options['host']} #{SSH_OPTIONS} 'sudo /sbin/ip addr add #{FLOATING_IP}#{FLOATING_IP_CIDR} dev #{INTERFACE}'`
        end
        if $?.exitstatus == 0
            true
        else
            false
        end
    end
end

class RedisRoleSwapContext
    attr_accessor :statemachine
    def initialize
        if (CONFIG['floating_ip'] == nil || CONFIG['floating_ip_cidr'] == nil)
            puts "\nCluster config is missing floating ip information.\n".red
            exit 1
        end
        # Gather info
        r1 = MyRedis.new(CONFIG['instance_one'])
        r2 = MyRedis.new(CONFIG['instance_two'])
        instances = [r1, r2]
        puts "\nCurrent cluster configuration:\n".white
        puts "Floating IP: ".white + CONFIG['floating_ip'] + "\n\n"

        instances.sort {|x,y| x.role <=> y.role}.each do |inst|
            puts "#{inst.role.capitalize}:".white + " #{inst.hostname}:#{inst.options['port']}"
            puts "Redis Replication Role: #{inst.redis_rep_role}"
            puts "Floating IP Role: #{inst.ip_role}"
            puts "Redis Version: [#{inst.version}]"
            puts "Read-Only: #{inst.read_only?}"
            puts "Arping Path: #{inst.arping_path}\n\n"
            if inst.role == "slave"
                unless @slave
                    @slave = inst
                else
                    puts "Boths of your servers look to have the slave role. Please check your configuration."
                    exit 1
                end
            elsif inst.role == "master"
                unless @master
                    @master = inst
                else
                    puts "Boths of your servers look to have the master role. Please check your configuration."
                    exit 1
                end
            end
        end

        if @slave && !@master
            puts "Master unavailble.\n".red
            exit 1
        elsif !@slave && @master
            puts "Slave unavailable.\n".red
            exit 1
        elsif !@slave && !@master
            puts "Master unavailble.\n".red
            puts "Slave unavailable.\n".red
            exit 1
        end
    end

    def check_configuration
        if !@master.read_only? && @slave.read_only?
            if @master.arping_path && @slave.arping_path
                @statemachine.good_config
            else
                @statemachine.bad_config
                puts "\nPreflight checks:\n".white
                puts "    Cannot find the arping binary on one servers.".red
                eixt 1
            end
        else
            puts "\nPreflight checks:\n".white
            puts "    Redis configruation... failed. The read/write states of the master and slave are wrong.".red
            @statemachine.bad_config
            exit 1
        end
    end

    # Return false if replication is not ok, or replication lag time.
    # If replication lag is below 1 second, we think it's ok.
    def replication_lag(slave, master)
        # First, check the first synchronization happended with success, if success,
        # the initial RDB was loaded from master.
        if slave.info("replication")["master_link_status"] != "up"
            return false
        end
        # Use PUBLISH/SUBSCRIBE to make sure replication is ok
        Thread.new do
            sleep 1
            master.publish("REPLICATION", Time.now.to_f)
        end
        begin
            Timeout.timeout(2) do
                slave.subscribe("REPLICATION") do |on|
                    on.message do |channel, msg|
                        lag = Time.now.to_f - msg.to_f
                        if lag < 1
                            return lag
                        else
                            return false
                        end
                    end
                end
            end
        rescue Timeout::Error
            return false
        end
    end

    def check_replication
        lagtime = replication_lag(@slave, @master) 
        @slave.info() # hack fix: https://github.com/redis/redis-rb/issues/323
        if lagtime != false && lagtime < 1
            @statemachine.replication_ready
        else
            @statemachine.replication_not_ready
            exit 1
        end
    end

    def verify_before_shutdown_master
        puts "Verifying before shutdown old master..."
        # 1. Check master is receving queires or not....
        # We need to use a separate client to monitor, cuz we cannot call other
        # commands in monitor status
        monitor_client = MyRedis.new(@master.options)
        while true
            begin
                Timeout.timeout(3) do
                    monitor_client.monitor() do |on|
                        if on == "OK"
                            next
                        end
                        puts on
                        break
                    end
                end
            rescue Timeout::Error
                puts "Master is no longer receiving queries in 3 secs now...OK"
                break
            end
            puts "Master is receiving queries now... sleep 1 sec, then check again."
            sleep 1
        end
        monitor_client.quit()
        # 2. monitor the master is no longer receiving any query
        while true
            lagtime = replication_lag(@slave, @master)
            @slave.info() # hack fix: https://github.com/redis/redis-rb/issues/323
            if lagtime == false || lagtime >= 1
                puts "Replication lags, sleep 1 sec, then check again."
                sleep 1
            else
                puts "Replication is ok."
                break
            end
        end
        @statemachine.verified_ok
    end

    def confirm?(question)
        $stdout.print question + "? (Y/N) "
        answer = $stdin.readline.chomp!
        case answer
            when "Y", "y"
                true
            when "N", "n"
                @statemachine.exit
            exit 3
        else
            puts "This is a Y or N question. OK?"
            confirm?(question)
        end
    end

    def prompt_user
        unless CHOICES[:check].nil?
            exit 0
        else
            puts "***WARNING***".red
            puts "Before you switch master, make sure all lua scripts are loaded on slaves!".red
            puts "\n"
            if confirm?("You ready to switch the roles")
                puts "\n\n"
                @statemachine.start_switching_roles
            end
        end
    end

    def remove_vip_from_master
        if @master.remove_vip
            puts "Remove vip from master...OK, no one can connect to master through VIP."
            @statemachine.next_set_master_readonly 
        else
            puts "Remove vip from master...FAIL"
            @statemachine.failed_to_remove_vip_from_master
        end
    end

    def set_master_readonly
        # if master is master, it's writable even if slave-read-only is enable.
        # We set it to yes, to make sure when it's demoted to slave, no one can write data to it
        if @master.config("set", "slave-read-only", "yes") == "OK"
            puts "Set master to read-only...OK"
            @statemachine.do_set_slave_writable
        else
            throw "cannot do config set on master"
        end
    end

    def set_slave_writable
        @slave.config("set", "slave-read-only", "no")
        @statemachine.add_vip_to_slave
    end

    def add_vip_to_slave
        if @slave.add_vip
            puts "Add vip to new master (existing slave)...OK"
            @statemachine.do_arping
        else
            puts "Add vip to new master (existing slave)...FAIL"
            exit 1
        end
    end

    def promote_slave_to_master
        @slave.slaveof("no", "one")
        puts "Promote slave to master...OK"
        @statemachine.shutdown_old_master
    end

    def arping_from_slave
        @slave.arping
        puts "Arping 4x...OK"
        @statemachine.verify_before_shutdown_master
    end

    def shutdown_old_master
        @master.shutdown()
        @statemachine.done
    end

    def done
        puts "You have successfully switched roles!\n".white
    end
end

redis_rep_role_swap = Statemachine.build do
    state :unknown do
        event :bad_config, :configuration_fail
        event :good_config, :configuration_ok
        on_entry :check_configuration
    end
    state :configuration_ok do
        event :replication_not_ready, :replication_not_ready, Proc.new { puts "Replication is NOT up to date, or slave is not running! Please catch up before changing roles.".red }
        event :replication_ready, :cluster_ready
        on_entry :check_replication
    end
    state :cluster_ready do
        event :start_switching_roles, :switch_roles, Proc.new { puts "Switching Roles:\n\n".white }
        event :exit, :exit, Proc.new { puts "\nFAIL: We are SO done here. You said NO.\n" }
        on_entry :prompt_user
    end 
    state :switch_roles do
        event :next_set_master_readonly, :do_set_master_readonly
        event :failed_to_remove_vip_from_master, :vip_removal_fail, Proc.new { puts "Failed to remove vip from master....FAIL".red}
        on_entry :remove_vip_from_master
    end
    state :do_set_master_readonly do
        event :do_set_slave_writable, :do_set_slave_writable
        on_entry :set_master_readonly
    end
    state :do_set_slave_writable do
        event :add_vip_to_slave, :add_vip_to_slave
        on_entry :set_slave_writable
    end
    state :add_vip_to_slave do
        event :do_arping, :do_arping
        on_entry :add_vip_to_slave
    end
    state :do_arping do
        event :verify_before_shutdown_master, :verify_before_shutdown_master
        on_entry :arping_from_slave
    end
    state :verify_before_shutdown_master do
        event :verified_ok, :promote_slave_to_master
        on_entry :verify_before_shutdown_master
    end
    state :promote_slave_to_master do
        event :shutdown_old_master, :shutdown_old_master
        on_entry :promote_slave_to_master
    end
    state :shutdown_old_master do
        event :done, :done, :done
        on_entry :shutdown_old_master
    end
    context RedisRoleSwapContext.new
end
