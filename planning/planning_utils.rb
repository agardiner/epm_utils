require 'cgi'


module PlanningUtils

    module ClassMethods

        # Parse a command-line dimension specification
        def parse_dimensions(arg, val, hsh)
            hsh[:top_members] = {}
            dims = val.split(',').map do |dim|
                if dim =~ /^([^:]+):(.+)$/
                    dim, top = $1, $2
                    if File.exists?(top)
                        hsh[:top_members][dim] = IO.readlines(top).map{ |line| line.chomp }
                    else
                        hsh[:top_members][dim] = top.split('~')
                    end
                end
                dim
            end
        end

    end

    def self.included(base)
        base.extend(ClassMethods)
    end


    # Returns an extract file path, given a dimension name (or object type),
    # top member(s), and whether the extract is level based.
    def get_extract_file(name, options = {})
        folder = options.fetch(:folder, arguments.output_dir)
        top_mbrs = options.fetch(:top_mbrs, nil)
        level_label = options.fetch(:level_based, false) ? '_Levels' : ''
        extension = options.fetch(:extension, arguments.respond_to?(:format) ?
                                  arguments.format : :csv).to_s.downcase
        selection_label = case
        when top_mbrs.nil? || top_mbrs.size == 1 && top_mbrs.first == name then ''
        when top_mbrs.size == 1 then "_#{top_mbrs.first}"
        else '_Subset'
        end
        '%s\\%s%s%s_Extract.%s' % [folder, name, selection_label, level_label, extension]
    end


    # Define a hash to hold artifacts to be migrated. The key to the hash is the
    # full path to the item in LCM terms, and the value is a hash containing
    # flags indicating whether the item is to be migrated and/or deleted.
    def migration_artifacts
        @migration_artifacts ||= Hash.new{ |h, k| h[k] = {} }
    end


    def gen_lcm_export(def_path, extract_path, project, application, options = {})
        log.info "Generating LCM export definition..."
        count = gen_lcm_definition(def_path, extract_path, project, application,
                    migration_artifacts, 'AppConnection', 'FileSystemConnection',
                    options.fetch(:recurse, false), options)
        log.detail "Output export definition for #{count} artifacts"
    end


    def gen_lcm_import(def_path, extract_path, project, application, options = {})
        log.info "Generating LCM import definition..."
        count = gen_lcm_definition(def_path, extract_path, project, application,
                    migration_artifacts, 'FileSystemConnection', 'AppConnection',
                    options.fetch(:recurse, false), options)
        log.detail "Output import definition for #{count} artifacts"
    end


    def gen_delete_list(def_path)
        log.info "Generating Planning_Deleter definition..."
        delete_artifacts = Hash.new{ |h, k| h[k] = [] }
        count = 0
        migration_artifacts.keys.sort.each do |key|
            fields = key.split('/')
            opts = migration_artifacts[key]
            if opts[:delete]
                artifact_type = case key
                when %r{^/Global Artifacts/Business Rules/([^/]+)/} then $1
                when %r{^/Global Artifacts/Task Lists/} then 'Task Lists'
                when %r{^/Global Artifacts/Composite Forms/} then 'Composite Forms'
                when %r{^/Plan Type/[^/]+/Data Forms/} then 'Data Forms'
                else
                    log.warn "Unknown artifact type for deletion: #{key}"
                    nil
                end
                if artifact_type
                    delete_artifacts[artifact_type] << fields.last
                    count += 1
                end
            end
        end
        file = File.open(def_path, "w:utf-8")
        file.write(delete_artifacts.to_yaml)
        file.close
        log.detail "Output #{count} deletions"
    end


    private

    def gen_lcm_definition(def_path, extract_path, project, application, artifacts, source, target, recursive, options)
        description = File.nameonly(def_path).gsub('_', ' ')
        raise "Shared Services Project folder must be specified" unless project
        raise "Application name must be specified" unless application
        template = <<-END
          <?xml version="1.0" encoding="UTF-8" ?>
          <Package name="web-migration" description="#{description}">
            <LOCALE>en_GB</LOCALE>
            <Connections>
              <ConnectionInfo name="HSSConnection" type="HSS" description="Hyperion Shared Service connection"
                              user="#{options[:user_id]}" password="#{options[:password]}" />
              <ConnectionInfo name="FileSystemConnection" type="FileSystem" description="File system connection"
                              HSSConnection="HSSConnection" filePath="#{File.nameonly(extract_path)}" />
              <ConnectionInfo name="AppConnection" type="Application" product="HP" project="#{project}"
                              application="#{application}" HSSConnection="HSSConnection"
                              description="Planning Application connection" />
            </Connections>
            <Tasks>
              <Task seqID="-1">
                <Source connection="#{source}">
                  <Options />
        END
        count = 0
        artifacts.keys.sort.each do |key|
            opts = migration_artifacts[key]
            if opts[:migrate]
                fields = key.split('/')
                path = case
                when recursive && fields.first == 'Global Artifacts' then fields[0..1]
                when recursive && fields.first == 'Plan Type' then fields[0..2]
                else fields[0..-2]
                end.join('/')
                artifact = fields.last
                template << %Q{#{' ' * 18}<Artifact recursive="#{recursive}" parentPath="#{CGI.escapeHTML(path)}" pattern="#{CGI.escapeHTML(artifact)}" />\n}
                count += 1
            end
        end
        template += <<-END
                </Source>
                <Target connection="#{target}">
                  <Options />
                </Target>
              </Task>
            </Tasks>
          </Package>
        END
        template.gsub!(/^ {10}/, '')

        log.detail "Generating LCM definition to #{def_path}"
        file = File.open(def_path, "w:utf-8|bom")
        file.write(template)
        file.close

        count
    end

end

