require 'batch/job'
#require 'axlsx'
require 'fileutils'
require 'sequel'
require 'csv'
require_relative 'planning/extractor'
require_relative 'planning/planning_utils'


class PlanningExtractor < Batch::Job

    include PlanningUtils


    configure 'connections.yaml'

    positional_arg :application, 'The name of the Planning application from which content should ' +
        'be extracted', short_key: '-a'

    flag_arg :outline_load, 'Generate OutlineLoad compatible dimension extracts',
        short_key: '-o', usage_break: 'Extracts:'
    flag_arg :levels, 'Generate level-based dimension extracts for visualising hierarchies',
        short_key: '-l'
    flag_arg :task_lists, 'Generate a task list extract, detailing the usage of forms, ' +
        'business rules etc on task lists', short_key: '-t'
    keyword_arg :forms, 'Generate a forms extract, detailing forms, form layouts, etc',
        short_key: '-f', value_optional: true, usage_value: 'FORM_PATTERN', default: false
    flag_arg :smart_lists, 'Extract Planning smart lists',
        short_key: 'L'
    flag_arg :menu_items, 'Extract Planning menu items',
        short_key: 'M'
    flag_arg :user_variables, 'Extract Planning user variables',
        short_key: 'V'
    flag_arg :security_access, 'Generate a secFile.txt extract suitable for use with the ' +
        'ImportSecurity Planning utility', short_key: '-S'

    keyword_arg :projects, 'Generate extracts of Business Rules projects matching PROJ_PATTERN, ' +
        'or all projects if no pattern is specified',
        value_optional: true, usage_value: 'PROJ_PATTERN', default: false,
        on_parse: :split_to_array
    keyword_arg :sequences, 'Generate extracts of Business Rules sequences matching SEQ_PATTERN, ' +
        'or all sequences if no pattern is specified',
        value_optional: true, usage_value: 'SEQ_PATTERN', default: false,
        on_parse: :split_to_array
    keyword_arg :rules, 'Generate extracts of Business Rules rules matching RULE_PATTERN, ' +
        'or all rules if no pattern is specified',
        value_optional: true, usage_value: 'RULE_PATTERN', default: false,
        on_parse: :split_to_array
    keyword_arg :macros, 'Generate extracts of Business Rules macros matching MACRO_PATTERN, ' +
        'or all macros if no pattern is specified',
        value_optional: true, usage_value: 'MACRO_PATTERN', default: false,
        on_parse: :split_to_array
    keyword_arg :variables, 'Generate extracts of Business Rules variables matching VAR_PATTERN, ' +
        'or all variables if no pattern is specified',
        value_optional: true, usage_value: 'VAR_PATTERN', default: false,
        on_parse: :split_to_array
    flag_arg :business_rules, 'Shortcut to extract ALL Business Rules sequences, rules, macros, and variables',
        short_key: '-b', on_parse: lambda{ |val, arg, hsh|
            hsh[:projects] ||= nil
            hsh[:sequences] ||= nil
            hsh[:rules] ||= nil
            hsh[:macros] ||= nil
            hsh[:variables] ||= nil
        }

    flag_arg :security_groups, 'Generate a Shared Services group configuration extract',
        short_key: '-G'
    flag_arg :security_users, 'Generate a Shared Services user configuration extract',
        short_key: '-U'

    flag_arg :sub_vars, 'Generate a Maxl script for creating/updating substitution variables',
        short_key: '-v'
    flag_arg :udfs, 'Generate dimension build files for setting formulas on HSP_UDF '+
        'tagged members', short_key: '-u'

    require_any_of :outline_load, :levels, :task_lists, :forms, :smart_lists, :menu_items,
        :user_variables, :security_access,
        :projects, :sequences, :rules, :macros, :variables, :business_rules,
        :security_groups, :security_users, :sub_vars, :udfs

    usage_break 'General Options:'
    keyword_arg :output_dir, 'The directory where extracts should be created',
        short_key: '-p'
    keyword_arg :dimensions, 'The dimension(s) to be extracted in OutlineLoad or level format. ' +
            'Each dimension may be followed by a list of top members, in the form of ' +
            'DIMENSION:MEMBER1[~MEMBER2...], or may specify the name of a file to be processed to ' +
            'obtain the member list using the form DIMENSION:FILE. If a file is used, it should contain ' +
            'a single member per line.',
            short_key: '-d', on_parse: lambda{ |val, arg, hsh| parse_dimensions(arg, val, hsh) }
    keyword_arg :format, 'The format in which to generate the extracts; may be TEXT or XLSX',
        default: 'TEXT', on_parse: lambda { |val, arg, hsh| val =~ /^xlsx?/i ? :xlsx : :csv }
    keyword_arg :mail_to, 'Send extract(s) in an email to RECIPIENTS (a comma separated list of email addresses)',
        short_key: 'm', usage_value: 'RECIPIENTS'
    flag_arg :lcm, 'Generate Shared Services LCM export definition'
    flag_arg :lcm_include_dependents, 'Include dependents of objects in LCM export definition'


    def extractor
        unless @extractor
            # Establish connections
            if RUBY_ENGINE == 'jruby'
                require 'java'
                require 'ojdbc6.jar'
            else
                require 'oci8'
            end
            db_conn = Sequel.connect(config.planning[arguments.application])
            @extractor = Hyperion::Planning::Extractor.new(db_conn)
        end
        @extractor
    end


    def xl
        @xl ||= xl_create_workbook
    end


    def finalise_extract(out, freeze_cols = 1, max_width = 40)
        if arguments.format == :xlsx
            out.column_info.each_with_index do |ci, i|
                ci.width = max_width if i >= freeze_cols && ci.width > max_width
            end
            xl_filter_and_freeze(out, freeze_cols)
        else
            out.close
        end
    end


    def gen_migration_scripts
        log.info "Generating LCM migration and deletion scripts..."

        # Generate LCM export and import migration definition files
        gen_lcm_export("#{arguments.output_dir}\\LCM_Export.xml",
                       "#{arguments.output_dir}\\LCM_Extract",
                       config.projects[arguments.application], arguments.application)
        gen_lcm_import("#{arguments.output_dir}\\LCM_Import.xml",
                       "#{arguments.output_dir}\\LCM_Extract",
                       config.projects[arguments.application], arguments.application)

        # Generate a list of items to be deleted
        gen_delete_list("#{arguments.output_dir}\\LCM_Delete.yaml")
    end


    def br_conn
        unless @br
            require 'hyperion/business_rules'
            @br = Hyperion::BusinessRules.new
            @br.connect(config.essbase_user, config.essbase_pwd, config.eas_server)
        end
        @br
    end


    def process_br_dependents(br, obj)
        bean_type = obj.is_a?(Java::ComHyperionHbrBeans::CoreBean) ? obj.bean_type : obj.core.bean_type
        br.send("get_#{bean_type}_usage".intern, obj).each do |dep|
            path = "/Global Artifacts/Business Rules/#{dep.bean_type.titleize}s/#{dep.name}"
            unless migration_artifacts[path].size > 0
                migration_artifacts[path] = { migrate: true, delete: true, is_dependent: true }
                process_br_dependents(br, dep)
            end
        end
    end


    def extract_hsp_udf(cube, dim_name, extract_file)
        count = 0
        mbr_sel = cube.open_member_selection("MemberQuery")
        begin
            mbr_sel.execute_query("<OutputType Binary <SelectMbrInfo(MemberName, ParentMemberName, MemberFormula)",
                                  %Q{@UDA("#{dim_name}", "HSP_UDF")})
            mbrs = mbr_sel.get_members
            if mbrs
                file = CSV.open(extract_file, 'w:utf-8')
                file << ["#{dim_name.upcase}_PARENT", "#{dim_name.upcase}_CHILD", "#{dim_name.upcase}_FORMULA"]
                mbrs.get_all.each do |mbr|
                    file << [mbr.parent_member_name, mbr.name, mbr.formula]
                    count += 1
                end
                file.close
            end
        ensure
            mbr_sel.close
        end
        count
    end


    def css
        unless @css
            begin
                require 'hyperion/css'
            rescue Hyperion::ConfigurationError
                log.error "Unable to connect to Shared Services on this server; this can only be " +
                    "run on a server with Foundation Services or Planning installed"
                return
            end
            @css = Hyperion::CSS.new
            @css.authenticate config.essbase_user, config.essbase_pwd
        end
        @css
    end


    def save_workbook(label)
        if @xl
            extract_file = get_extract_file(label)
            @xl.serialize(extract_file)
            @extracts << extract_file
            @xl = nil
        end
    end


    desc 'Generates dimension extracts in OutlineLoad compatible format'
    task :outline_load_extract do |extractor|
        arguments.dimensions ||= extractor.dimension_names.keys

        xl = nil
        arguments.dimensions.each do |dim_name|
            top_mbrs = arguments.respond_to?(:top_members) ?
                arguments.top_members[dim_name] || [dim_name] : [dim_name]
            if arguments.format == :xlsx
                # A single workbook for all dimensions
                xl ||= xl_create_workbook
                xl.workbook.add_worksheet(:name => dim_name) do |sheet|
                    extractor.extract_dimension(sheet, dim_name, top_mbrs,
                                               :header_style => xl_styles['Title'],
                                               :log => log)
                    xl_filter_and_freeze(sheet, 2)
                end
            else
                # A .CSV file per dimension
                extract_file = get_extract_file(dim_name, :top_mbrs => top_mbrs)
                extractor.extract_dimension(extract_file, dim_name, top_mbrs)
                @extracts << extract_file
            end
        end
        if xl
            extract_file = get_extract_file('Dimensions')
            xl.serialize(extract_file)
            @extracts << extract_file
        end
    end


    desc 'Generates dimension extracts with members indented to better visualise parent/child relationships'
    task :level_extract do |extractor|
        arguments.dimensions ||= extractor.dimension_names.keys

        xl = nil
        arguments.dimensions.each do |dim_name|
            top_mbrs = arguments.respond_to?(:top_members) ?
                arguments.top_members[dim_name] || [dim_name] : [dim_name]
            if arguments.format == :xlsx
                # A single workbook for all dimensions
                xl ||= xl_create_workbook
                xl.workbook.add_worksheet(:name => dim_name) do |sheet|
                    extractor.extract_dimension_levels(sheet, dim_name, top_mbrs,
                                                      :header_style => xl_styles['Title'],
                                                      :log => log)
                    xl_filter_and_freeze(sheet, 2)
                end
            else
                # A .CSV file per dimension
                extract_file = get_extract_file(dim_name,
                                                :top_mbrs => top_mbrs, :level_based => true)
                extractor.extract_dimension_levels(extract_file, dim_name, top_mbrs)
                @extracts << extract_file
            end
        end
        if xl
            extract_file = get_extract_file('Levels')
            xl.serialize(extract_file)
            @extracts << extract_file
        end
    end


    desc 'Generates a task list report'
    task :task_list_extract do |extractor|
        if arguments.format == :xlsx
            xl.workbook.add_worksheet(:name => 'Task Lists') do |sheet|
                extractor.extract_task_lists(sheet,
                                            :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                            :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 3)
            end
        else
            extract_file = get_extract_file('Task_Lists')
            extractor.extract_task_lists(extract_file, :field_sep => ',',
                                         :strip_line_breaks => true)
            @extracts << extract_file
        end
    end


    desc 'Generates a smart lists report'
    task :smart_list_extract do |extractor|
        if arguments.format == :xlsx
            xl.workbook.add_worksheet(:name => 'Smart Lists') do |sheet|
                extractor.extract_smart_lists(sheet,
                        :header_map => lambda{ |hdr| hdr.to_s.titleize },
                        :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 1)
            end
            xl.workbook.add_worksheet(:name => 'Smart List Items') do |sheet|
                extractor.extract_smart_list_items(sheet,
                        :header_map => lambda{ |hdr| hdr.to_s.titleize },
                        :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 1)
            end
        else
            extract_file = get_extract_file('Smart_Lists')
            extractor.extract_smart_lists(extract_file, :field_sep => ',')
            @extracts << extract_file
            extract_file = get_extract_file('Smart_List_Items')
            extractor.extract_smart_list_items(extract_file, :field_sep => ',')
            @extracts << extract_file
        end
    end


    desc 'Generates a menu items report'
    task :menu_items_extract do |extractor|
        if arguments.format == :xlsx
            xl.workbook.add_worksheet(:name => 'Menu Items') do |sheet|
                extractor.extract_menu_items(sheet,
                                            :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                            :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 3)
            end
        else
            extract_file = get_extract_file('Menu_Items')
            extractor.extract_menu_items(extract_file, :field_sep => ',')
            @extracts << extract_file
        end
    end


    desc 'Generates a user variables report'
    task :user_variables_extract do |extractor|
        if arguments.format == :xlsx
            xl.workbook.add_worksheet(:name => 'User Variables') do |sheet|
                extractor.extract_user_variables(sheet,
                                                 :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                                 :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 1)
            end
        else
            extract_file = get_extract_file('User_Variables')
            extractor.extract_user_variables(extract_file, :field_sep => ',')
            @extracts << extract_file
        end
    end


    desc 'Generates a form audit report'
    task :forms_extract do |extractor, form_pattern|
        pattern = form_pattern ? form_pattern.gsub('*', '%').gsub('?', '_') : '%'
        lcm_proc = nil
        if arguments.lcm
            lcm_proc = lambda do |rec|
                path = case rec[1]
                when 'Simple' then "/Plan Type/#{rec[1]}/Data Forms#{rec[2]}/#{rec[0]}"
                when 'Composite' then "/Global Artifacts/Composite Forms#{rec[2]}/#{rec[0]}"
                end
                migration_artifacts[path] = {migrate: true, delete: true}
            end
        end
        if arguments.format == :xlsx
            xl.workbook.add_worksheet(:name => 'Forms') do |sheet|
                extractor.extract_forms(sheet, pattern,
                                       :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                       :header_style => xl_styles['Title'], &lcm_proc)
                xl_filter_and_freeze(sheet, 3)
            end
            xl.workbook.add_worksheet(:name => 'Composite Form Layout') do |sheet|
                extractor.extract_composite_form(sheet, pattern,
                                             :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                             :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 3)
            end
            xl.workbook.add_worksheet(:name => 'Form Layout') do |sheet|
                extractor.extract_form_layout(sheet, pattern,
                                             :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                             :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 3)
            end
            xl.workbook.add_worksheet(:name => 'Form Members') do |sheet|
                extractor.extract_form_members(sheet, pattern,
                                             :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                             :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 3)
            end
            xl.workbook.add_worksheet(:name => 'Form Calcs') do |sheet|
                extractor.extract_form_calcs(sheet, pattern,
                                            :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                            :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 2)
            end
            xl.workbook.add_worksheet(:name => 'Form Menus') do |sheet|
                extractor.extract_form_menus(sheet, pattern,
                                            :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                            :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 1)
            end
        else
            extract_file = get_extract_file('Forms')
            extractor.extract_forms(extract_file, pattern, :field_sep => ',', &lcm_proc)
            @extracts << extract_file
            extract_file = get_extract_file('Form_Composite_Layout')
            extractor.extract_composite_form(extract_file, pattern, :field_sep => ',')
            @extracts << extract_file
            extract_file = get_extract_file('Form_Layout')
            extractor.extract_form_layout(extract_file, pattern, :field_sep => ',')
            @extracts << extract_file
            extract_file = get_extract_file('Form_Members')
            extractor.extract_form_members(extract_file, pattern, :field_sep => ',')
            @extracts << extract_file
            extract_file = get_extract_file('Form_Calcs')
            extractor.extract_form_calcs(extract_file, pattern, :field_sep => ',')
            @extracts << extract_file
            extract_file = get_extract_file('Form_Menus')
            extractor.extract_form_menus(extract_file, pattern, :field_sep => ',')
            @extracts << extract_file
        end
        if arguments.lcm_include_dependents
            log.info "Locating form dependents..."
            extractor.get_form_usage(pattern) do |task_list, form, folder|
                path = task_list ?
                    "/Global Artifacts/Task Lists/#{task_list}" :
                    "/Global Artifacts/Composite Forms#{folder}/#{form}"
                migration_artifacts[path] = {migrate: true, delete: true}
            end
        end
    end


    desc 'Generate a Business Rules Projects extract'
    task :projects_extract do
        log.info "Extracting Business Rules projects..."
        headers = ['Project Name', 'Project Contents', 'Access Privileges', 'Used By']
        lcm_proc = nil
        count = 0
        br = br_conn
        extract_file = nil

        if arguments.format == :xlsx
            out = xl.workbook.add_worksheet(:name => 'Projects')
            out.add_row(headers, :style => xl_styles['Title'])
        else
            extract_file = get_extract_file('Business_Rule_Projects')
            out = CSV.open(extract_file, 'w:utf-8')
            out << headers
        end

        if arguments.lcm
            lcm_proc = lambda do |proj|
                migration_artifacts["/Global Artifacts/Business Rules/Projects/#{proj.name}"] = {
                    migrate: true, delete: true
                }
                process_br_dependents(br, proj) if arguments.lcm_include_dependents
            end
        end

        projs = br.extract_projects(arguments.projects, &lcm_proc).sort_by(&:name)
        projs.each_with_index do |proj, i|
            Console.show_progress("Extracting project #{proj.name}", i, projs.length)
            access = br.get_object_access(proj)
            usage = br.get_project_usage(proj)
            use = br.get_project_object_use(proj)
            out << [
                proj.name,
                use.to_a.join("\n"),
                access.sort.join("\n"),
                usage.map(&:name).sort.join("\n")
            ]
            count += 1
        end
        Console.clear_progress
        finalise_extract(out)
        log.detail "Extracted #{count.with_commas} projects"
        @extracts << extract_file if extract_file
    end


    desc 'Generate a Business Rules Sequences extract'
    task :sequences_extract do
        log.info "Extracting Business Rules sequences..."
        headers = ['Sequence Name', 'Sequence Items', 'Access Privileges', 'Used By Sequence(s)']
        lcm_proc = nil
        count = 0
        br = br_conn
        extract_file = nil

        if arguments.format == :xlsx
            out = xl.workbook.add_worksheet(:name => 'Sequences')
            out.add_row(headers, :style => xl_styles['Title'])
        else
            extract_file = get_extract_file('Business_Rule_Sequences')
            out = CSV.open(extract_file, 'w:utf-8')
            out << headers
        end

        if arguments.lcm
            lcm_proc = lambda do |seq|
                migration_artifacts["/Global Artifacts/Business Rules/Sequences/#{seq.name}"] = {
                    migrate: true, delete: true
                }
                process_br_dependents(br, seq) if arguments.lcm_include_dependents
            end
        end

        seqs = br.extract_sequences(arguments.sequences, &lcm_proc).sort_by(&:name)
        seqs.each_with_index do |seq, i|
            Console.show_progress("Extracting sequence #{seq.name}", i, seqs.length)
            access = br.get_object_access(seq)
            usage = br.get_sequence_usage(seq)
            out << [
                seq.name,
                seq.elements.to_a.join(', '),
                access.sort.join("\n"),
                usage.map(&:name).sort.join("\n")
            ]
            count += 1
        end
        Console.clear_progress
        finalise_extract(out)
        log.detail "Extracted #{count.with_commas} sequences"
        @extracts << extract_file if extract_file
    end


    desc 'Generate a Business Rules extract'
    task :rules_extract do
        log.info "Extracting business rules..."
        headers = ['Rule Name', 'Runtime Prompts', 'Access Privileges',
                   'Used By Sequence(s)', 'Macro(s) Used', 'Rule Text']
        lcm_proc = nil
        count = 0
        br = br_conn
        extract_file = nil

        if arguments.format == :xlsx
            out = xl.workbook.add_worksheet(:name => 'Rules')
            out.add_row(headers, :style => xl_styles['Title'])
        else
            extract_file = get_extract_file('Business_Rules')
            out = CSV.open(extract_file, 'w:utf-8')
            out << headers
        end

        if arguments.lcm
            lcm_proc = lambda do |rule|
                migration_artifacts["/Global Artifacts/Business Rules/Rules/#{rule.name}"] = {
                    migrate: true, delete: true
                }
                process_br_dependents(br, rule) if arguments.lcm_include_dependents
            end
        end

        rules = br.extract_rules(arguments.rules, &lcm_proc).sort_by(&:name)
        rules.each_with_index do |rule, i|
            Console.show_progress("Extracting rule #{rule.name}", i + 1, rules.length)
            prompts = br.get_rule_prompts(rule)
            access = br.get_object_access(rule)
            usage = br.get_rule_usage(rule)
            use = br.get_rule_macro_use(rule)
            out << [
                rule.name,
                prompts[1..-1].sort.join(', '),
                access.sort.join("\n"),
                usage.map(&:name).sort.join("\n"),
                use.map(&:name).sort.join("\n"),
                rule.body.rule_text
            ]
            count += 1
        end
        Console.clear_progress
        finalise_extract(out)
        log.detail "Extracted #{count.with_commas} business rules"
        @extracts << extract_file if extract_file
    end


    desc 'Generate a Business Rules macros extract'
    task :macros_extract do
        log.info "Extracting business rule macros..."
        headers = ['Macro Name', 'Macro Parameters', 'Access Privileges',
                   'Used By Rule(s)', 'Used By Macro(s)', 'Macro Text']
        lcm_proc = nil
        count = 0
        br = br_conn
        extract_file = nil

        if arguments.format == :xlsx
            out = xl.workbook.add_worksheet(:name => 'Macros')
            out.add_row(headers, :style => xl_styles['Title'])
        else
            extract_file = get_extract_file('Business_Rule_Macros')
            out = CSV.open(extract_file, 'w:utf-8')
            out << headers
        end

        if arguments.lcm
            lcm_proc = lambda do |macro|
                migration_artifacts["/Global Artifacts/Business Rules/Macros/#{macro.name}"] = {
                    migrate: true, delete: true
                }
                process_br_dependents(br, macro) if arguments.lcm_include_dependents
            end
        end

        macros = br.extract_macros(arguments.macros, &lcm_proc).sort_by(&:name)
        macros.each_with_index do |macro, i|
            Console.show_progress("Extracting macro #{macro.name}", i + 1, macros.length)
            access = br.get_object_access(macro)
            usage = br.get_macro_usage(macro)
            rule_usage, macro_usage = usage.partition{ |use| use.object_type == 1 }
            out << [
                macro.name,
                macro.params.split(',').join(', '),
                access.sort.join("\n"),
                rule_usage.map(&:name).sort.join("\n"),
                macro_usage.map(&:name).sort.join("\n"),
                macro.body.rule_text
            ]
            count += 1
        end
        Console.clear_progress
        finalise_extract(out)
        log.detail "Extracted #{count.with_commas} macros"
        @extracts << extract_file if extract_file
    end


    desc 'Generate a business rules variables extract'
    task :variables_extract do
        log.info "Extracting Business Rules Variables..."
        headers = ['Variable Name', 'Global/Local', 'Type', 'Smart List', 'Dimension', 'Selection',
                   'Usage Type', 'Prompt String', 'Access Privileges', 'Used By Rule(s)', 'Used By Macro(s)']
        lcm_proc = nil
        count = 0
        br = br_conn
        extract_file = nil

        if arguments.format == :xlsx
            out = xl.workbook.add_worksheet(:name => 'Variables')
            out.add_row(headers, :style => xl_styles['Title'])
        else
            extract_file = get_extract_file('Business_Rule_Variables')
            out = CSV.open(extract_file, 'w:utf-8')
            out << headers
        end

        if arguments.lcm
            lcm_proc = lambda do |var|
                unless var.local?
                    migration_artifacts["/Global Artifacts/Business Rules/Global Variables/#{var.name}"] = {
                        migrate: true, delete: true
                    }
                    process_br_dependents(br, var) if arguments.lcm_include_dependents
                end
            end
        end

        vars = br.extract_variables(arguments.variables, &lcm_proc).sort_by(&:name)
        vars.each_with_index do |var, i|
            Console.show_progress("Extracting variable #{var.name}", i + 1, vars.length)
            access = br.get_object_access(var)
            usage = br.get_variable_usage(var)
            rule_usage, macro_usage = usage.partition{ |use| use.object_type == 1 }
            out << [
                var.name,
                var.local? ? "Local (#{usage.first && usage.first.name})" : 'Global',
                var.language_type,
                var.enum_name,
                var.limits.dimension,
                var.limits.selected,
                case var.usage_type
                when 1 then 'Saved selection'
                when 2 then 'Use by value'
                when 3 then 'Run-time prompt'
                else var.usage_type
                end,
                var.prompt_string,
                access.sort.join("\n"),
                rule_usage.map(&:name).sort.join("\n"),
                macro_usage.map(&:name).sort.join("\n")
            ]
            count += 1
        end
        Console.clear_progress
        finalise_extract(out)
        log.detail "Extracted #{count.with_commas} variables"
        @extracts << extract_file if extract_file
    end


    desc 'Generate a Planning security access extract'
    task :security_access_extract do |extractor|
        if arguments.format == :xlsx
            xl.workbook.add_worksheet(:name => 'Security Access') do |sheet|
                extractor.extract_security(sheet,
                                          :header_map => lambda{ |hdr| hdr.to_s.titleize },
                                          :header_style => xl_styles['Title'])
                xl_filter_and_freeze(sheet, 3)
            end
        else
            # CSV format for secFile.txt excludes headers
            extract_file = get_extract_file('Security_Access')
            extractor.extract_security(extract_file, :field_sep => ',',
                                       :include_col_headers => false)
            @extracts << extract_file
        end
    end


    desc 'Generate a security group extract from Shared Services'
    task :security_groups_extract do
        log.info "Retrieving group provisioning details..."
        app_id = css.get_app_id 'HP', arguments.application
        prov = css.get_provisioning app_id
        groups = prov.groups.sort
        log.detail "Found #{groups.size} provisioned groups"

        log.info "Retrieving group membership..."
        extract_file = get_extract_file('Security_Groups')
        if arguments.format == :xlsx
            xl = xl_create_workbook
            xl.workbook.add_worksheet(:name => 'Security Groups') do |sheet|
                sheet.add_row ['Group', 'Identity', 'Child Groups'], :style => xl_styles['Title']
                groups.each do |group|
                    children = group.get_group_list
                    sheet.add_row [group, group.identity, children.sort.join("\n")]
                end
                sheet.column_info.each do |ci|
                    ci.width = 80 if ci.width > 80
                end
                xl_filter_and_freeze(sheet, 1)
            end
            xl.serialize(extract_file)
        else
            out = CSV.open(extract_file, 'w:utf-8')
            out << ['Group', 'Identity', 'Child Groups']
            groups.each do |group|
                children = group.get_group_list
                out << [group, group.identity, children.sort.join(", ")]
            end
            out.close
        end
        @extracts << extract_file
        log.detail "Output group details for #{groups.size} groups"
    end


    desc 'Generate a security user extract from Shared Services'
    task :security_users_extract do
        log.info "Retrieving group provisioning details..."
        app_id = css.get_app_id 'HP', arguments.application
        prov = css.get_provisioning app_id
        users = prov.users.sort
        log.detail "Found #{users.size} provisioned users"

        log.info "Retrieving user group membership..."
        extract_file = get_extract_file('Security_Users')
        if arguments.format == :xlsx
            xl = xl_create_workbook
            xl.workbook.add_worksheet(:name => 'Security Users') do |sheet|
                sheet.add_row ['User', 'Identity', 'Email', 'Groups'], :style => xl_styles['Title']
                users.each do |user|
                    groups = user.groups(false)
                    sheet.add_row [user, user.identity, user.email_addresses.join("\n"), groups.sort.join("\n")]
                end
                sheet.column_info.each do |ci|
                    ci.width = 80 if ci.width > 80
                end
                xl_filter_and_freeze(sheet, 1)
            end
            xl.serialize(extract_file)
        else
            out = CSV.open(extract_file, 'w:utf-8')
            out << ['User', 'Identity', 'Email', 'Groups']
            users.each do |user|
                groups = user.groups(false)
                out << [user, user.identity, user.email_addresses.join("; "), groups.sort.join(", ")]
            end
            out.close
        end
        @extracts << extract_file
        log.detail "Output user details for #{users.size} users"
    end


    desc 'Generate a substitution variables MaxL script'
    task :sub_vars_extract do
        log.info "Extracting substitution variables"
        extract_file = get_extract_file('Substitution_Variables', extension: :mxl)
        file = File.open(extract_file, 'w')

        file.puts "/* Server level substitution variables */"
        count = 0
        ess_srv = get_essbase_server()
        ess_srv.get_substitution_variables.sort{ |a, b| a[0] <=> b[0] }.each do |var|
            file.puts "alter system add variable '#{var[0]}';"
            file.puts "alter system set variable '#{var[0]}' '#{var[1]}';"
            count += 1
        end
        log.detail "Output #{count} system substitution variables"

        file.puts
        file.puts "/* Application level substitution variables */"
        count = 0
        ess_app = ess_srv.get_application(arguments.application)
        ess_app.get_substitution_variables.sort{ |a, b| a[0] <=> b[0] }.each do |var|
            if var[2]
                file.puts "alter application '#{arguments.application}' add variable '#{var[0]}';"
                file.puts "alter application '#{arguments.application}' set variable '#{var[0]}' '#{var[1]}';"
                count += 1
            end
        end
        log.detail "Output #{count} application substitution variables"

        file.puts
        file.puts "/* Database level substitution variables */"
        count = 0
        ess_app.get_cubes.get_all.each do |ess_cube|
            ess_cube.get_substitution_variables.sort{ |a, b| a[0] <=> b[0] }.each do |var|
                if var[2] && var[3]
                    file.puts "alter database '#{arguments.application}'.'#{ess_cube.name}' add variable '#{var[0]}';"
                    file.puts "alter database '#{arguments.application}'.'#{ess_cube.name}' set variable '#{var[0]}' '#{var[1]}';"
                    count += 1
                end
            end
            file.puts
        end
        log.detail "Output #{count} database substitution variables"
        file.close
        @extracts << extract_file
    end


    desc 'Generate an outline build file containing formulas for members with HSP_UDF UDA'
    task :ess_udf_extract do
        ess_srv = get_essbase_server()
        ess_app = ess_srv.get_application(arguments.application)
        ess_app.get_cubes.get_all.each do |cube|
            log.info "Extracting HSP_UDF formulas for #{cube.name}..."
            cube.get_dimensions.get_all.each do |dim|
                extract_file = get_extract_file("#{cube.name}_#{dim.name}_UDF_Formulas")
                count = extract_hsp_udf(cube, dim.name, extract_file)
                if count > 0
                    log.detail "Output #{count} formulas for #{cube.name} #{dim.name}"
                    @extracts << extract_file
                end
            end
            cube.clear_active
        end
    end


    task :send_email do
        zip_file = "#{arguments.output_dir}/#{arguments.application}_Extracts.zip"
        create_zip(zip_file, *@extracts)

        msg = create_html_email do |body|
            body << "<p>Please find attached the extracts you requested from the #{arguments.application} "
            body << "Planning application.</p>"
            body << "<br>"
        end
        msg.subject = "[Planning Extractor] Extracts from #{config.environment}/#{arguments.application}"
        msg.add_file(zip_file)
        msg.to = arguments.mail_to
        msg.deliver!
        log.detail "Email sent to #{recipient_count(msg)} recipients"
    end



    job 'Generates various different extracts of Planning metadata' do
        @extracts = []
        unless arguments.output_dir
            arguments.output_dir = "extracts\\#{arguments.application}"
        end
        FileUtils.mkdir_p(arguments.output_dir)

        log.info "Configuration settings:"
        log.config "Output path:                       #{arguments.output_dir}"
        log.config "Output format:                     #{arguments.format.to_s.upcase}"
        log.config "Output LCM Migration:              #{arguments.lcm ? 'Yes' : 'No'}"
        log.config "Outline load extracts:             #{arguments.outline_load ? 'Yes' : 'No'}"
        log.config "Level extracts:                    #{arguments.levels ? 'Yes' : 'No'}"
        log.config "Forms extract:                     #{arguments.forms != false ? 'Yes' : 'No'}"
        log.config "Smart lists extract:               #{arguments.smart_lists ? 'Yes' : 'No'}"
        log.config "Task list extract:                 #{arguments.task_lists ? 'Yes' : 'No'}"
        log.config "Menu Items extract:                #{arguments.menu_items ? 'Yes' : 'No'}"
        log.config "User Variables extract:            #{arguments.user_variables ? 'Yes' : 'No'}"
        log.config "Security Access extract:           #{arguments.security_access ? 'Yes' : 'No'}"
        log.config "Business Rules Projects extract:   #{arguments.projects != false ? 'Yes' : 'No'}"
        log.config "Business Rules Sequences extract:  #{arguments.sequences != false ? 'Yes' : 'No'}"
        log.config "Business Rules Rules extract:      #{arguments.rules != false ? 'Yes' : 'No'}"
        log.config "Business Rules Macros extract:     #{arguments.macros != false ? 'Yes' : 'No'}"
        log.config "Business Rules Variables extract:  #{arguments.variables != false ? 'Yes' : 'No'}"
        log.config "Security Groups extract:           #{arguments.security_groups ? 'Yes' : 'No'}"
        log.config "Security Users extract:            #{arguments.security_users ? 'Yes' : 'No'}"
        log.config "Sub Vars extract:                  #{arguments.sub_vars ? 'Yes' : 'No'}"
        log.config "Essbase UDF extract:               #{arguments.udfs ? 'Yes' : 'No'}"

        # Extract dimension metadata
        outline_load_extract(extractor) if arguments.outline_load
        level_extract(extractor) if arguments.levels

        # Extract Planning database content
        if (pattern = arguments.forms) != false
            forms_extract(extractor, pattern)
        end
        task_list_extract(extractor) if arguments.task_lists
        smart_list_extract(extractor) if arguments.smart_lists
        menu_items_extract(extractor) if arguments.menu_items
        user_variables_extract(extractor) if arguments.user_variables
        security_access_extract(extractor) if arguments.security_access
        save_workbook('Planning') if arguments.format == :xlsx

        # Extract business rules content
        projects_extract if arguments.projects != false
        sequences_extract if arguments.sequences != false
        rules_extract if arguments.rules != false
        macros_extract if arguments.macros != false
        variables_extract if arguments.variables != false
        save_workbook('Business_Rules') if arguments.format == :xlsx

        # Extract Shared Services content
        security_groups_extract if arguments.security_groups
        security_users_extract if arguments.security_users

        # Extract Essbase content
        sub_vars_extract if arguments.sub_vars
        ess_udf_extract if arguments.udfs

        br_conn.disconnect if @br

        # Generate LCM migration and deletion scripts
        if arguments.lcm && migration_artifacts.size > 0
            gen_migration_scripts
        end

        if arguments.mail_to
            send_email
        end
    end

end


PlanningExtractor.run if $0 == __FILE__

