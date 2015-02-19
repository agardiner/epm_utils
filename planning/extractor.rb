require_relative './sql'


class Hyperion::Planning

    class Extractor

        include Hyperion::Planning::SQL


        def initialize(conn)
            @conn = conn
        end


        def log
            @log ||= Batch::LogManager.logger('planning.extractor')
        end


        # Returns a Hash containing the names of the dimensions in the Planning app,
        # as well as the type of the dimension ('Dimension' or 'Attribute Dimension')
        def dimension_names
            unless @dimensions
                log.detail "Retrieving dimension names..."
                @dimensions = @conn.fetch(DIMENSION_SQL).to_hash(:dimension_name, :dimension_type)
            end
            @dimensions
        end


        # Get plan type names
        def plan_types
            unless @plan_types
                log.detail "Retrieving plan type names..."
                @plan_types = @conn.fetch(PLAN_TYPE_SQL).map(:plan_type)
            end
            @plan_types
        end


        # Extracts the current outline structure to a flat file, suitable for
        # loading via the Outline Load utility
        def extract_dimension(target, dim_name, top_members = [dim_name], options = {}, &block)
            if top_members.size == 1 && top_members.first == dim_name
                log.info "Extracting outline load metadata for #{dim_name}..."
            else
                log.info "Extracting outline load metadata for #{dim_name} below #{top_members.join(', ')}..."
            end

            aliases = Hash.new { |h, k| h[k] = {} }
            udas = Hash.new { |h, k| h[k] = [] }
            attrs = Hash.new { |h, k| h[k] = {} }
            attr_dims = []

            # Get aliases
            log.detail "Retrieving #{dim_name} aliases..."
            row_count = @conn.fetch(ALIAS_SQL, dim_name).each do |row|
                aliases[row[:alias_tbl_name]][row[:mbr_id]] = row[:alias]
            end
            alias_tbls = aliases.keys
            log.detail "Found #{row_count.count} aliases in #{alias_tbls.size} alias tables"

            dim_type = dimension_names[dim_name]
            unless dim_type == 'Attribute Dimension'
                # Get UDAs
                log.detail "Retrieving #{dim_name} UDA associations..."
                row_count = @conn.fetch(UDA_SQL, dim_name) do |row|
                    udas[row[:object_id]] << row[:uda_value]
                end
                log.detail "Found #{row_count.count} UDA associations"

                # Get attribute associations for each member
                log.detail "Retrieving attribute dimension associations..."
                row_count = @conn.fetch(ATTR_SQL, dim_name) do |row|
                    attrs[row[:attr_dim_name]][row[:mbr_id]] = row[:attr_name]
                end
                attr_dims = attrs.keys.sort
                log.detail "Found #{row_count.count} attribute associations in #{
                    attr_dims.count} attribute dimensions"
            end

            # Set extract options
            used_in_col = nil
            consol_op_col = nil
            default_options = {
                :set_headers => lambda { |headers|
                    headers.slice!(0)                       # Get rid of object_id
                    headers.insert(2, *(alias_tbls.map{ |tbl| "Alias: #{tbl}" }))  # Add alias columns
                    # Replace CONSOL_OP and USED_IN with plan-type specific fields
                    used_in_col = headers.index(:used_in)
                    consol_op_col = headers.index(:consol_op)
                    if consol_op_col
                        headers[consol_op_col, 1] = plan_types.map{ |pt| "Aggregation (#{pt})" }
                    end
                    if used_in_col
                        headers[used_in_col, 1] = plan_types.map{ |pt| "Plan Type (#{pt})" }
                    end
                    # Add headings for each associated attribute dimension
                    headers.concat(attr_dims)
                },
                :header_map => lambda { |field|
                    field = field.to_s
                    field.downcase == dim_name.downcase ? dim_name : titleize(field)
                },
                # OutlineLoad wants a byte-order-marker (BOM) so that it can detect
                # whether an input file is in UTF-8 or ASCII
                :encoding => 'utf-8|bom'
            }
            if target.is_a?(String)
                options[:field_sep] = ','
                options[:strip_line_breaks] = true
            end
            options = default_options.merge(options)

            # Get member properties
            log.detail "Retrieving #{dim_name} members..."
            row_count = 0
            rows = [] unless target
            top_members.each do |top_member|
                sql = get_member_ids_sql(dim_name, top_member)
                top_mbr_ids = @conn.fetch(sql).map(:member_id)
                if top_mbr_ids.length == 0
                    log.warn "No #{dim_name} member found with name '#{top_member}'"
                end
                top_mbr_ids.each do |top_mbr_id|
                    sql = dim_type == 'Attribute Dimension' ?
                        get_attr_dimension_sql(dim_name, top_mbr_id) :
                        get_dimension_sql(dim_name, top_mbr_id)
                    options[:append] = row_count > 0
                    options[:include_col_headers] = row_count == 0
                    row_count += execute_query(sql, target, options) do |row|
                        mbr_id = row.slice!(0)
                        row.insert(2, *(alias_tbls.map{ |tbl| aliases[tbl][mbr_id] }))
                        # Handle :used_in and :consol_op columns
                        if consol_op = (consol_op_col && row[consol_op_col].to_i)
                            row[consol_op_col, 1] = plan_types.each_with_index.map do |pt, i|
                            	case (127 << i * 6) & consol_op
			        when 0 then '+'
			        when 1 then '-'
			        when 2 then '*'
			        when 3 then '/'
			        when 4 then '%'
			        when 5 then '~'
			        when 6 then 'Never'
                                end
                            end
                        end
                        if used_in = (used_in_col && row[used_in_col])
                            row[used_in_col, 1] = plan_types.each_with_index.map do |pt, i|
                                (1 << i) & used_in ? 'True' : 'False'
                            end
                        end
                        # Add UDAs
                        row[-1] = udas[mbr_id].join(',') if row[-1] == 'UDA_Placeholder'
                        attr_dims.each do |attr_dim|
                            row << attrs[attr_dim][mbr_id]
                        end
                        # If a formula starts with an = sign and the target is Excel, we need to add a '
                        row[6] = "'#{row[6]}" if row[6] && row[6] =~ /^=/ && !target.is_a?(String)
                        rows << row unless target
                    end
                end
            end
            log.detail "Output #{row_count} #{dim_name} members"
            rows
        end


        def extract_dimension_levels(target, dim_name, top_members = [dim_name], options = {})
            log.info "Extracting level-based #{dim_name} extract..."

            # Set extract options
            default_options = {
                :header_map => lambda { |field| titleize(field.to_s) },
                :encoding => 'utf-8|bom'
            }
            if target.is_a?(String)
                options[:field_sep] = ','
                options[:strip_line_breaks] = true
            end
            options = default_options.merge(options)

            row_count = 0
            top_members.each do |top_member|
                sql = get_member_ids_sql(dim_name, top_member)
                top_mbr_ids = @conn.fetch(sql).map(:member_id)
                if top_mbr_ids.length == 0
                    log.warn "No #{dim_name} member found with name '#{top_member}'"
                end
                top_mbr_ids.each do |top_mbr_id|
                    options[:append] = row_count > 0
                    options[:include_col_headers] = row_count == 0
                    options[:bind_vars] = [dim_name, top_mbr_id]
                    row_count += execute_query(DIMENSION_BY_LEVEL_SQL, target, options) do |row|
                        path = row.pop
                        row.concat(path.split('|'))
                    end
                end
            end
            log.detail "Output #{row_count} #{dim_name} members"
        end


        def extract_task_lists(target, options = {}, &block)
            log.info "Extracting task lists..."
            row_count = execute_query(TASK_LIST_SQL, target, options, &block)
            log.detail "Output #{row_count} task list records"
        end


        def extract_forms(target, filter = '%', options = {}, &block)
            log.info "Extracting forms..."
            options[:bind_vars] = [filter]
            row_count = execute_query(FORMS_SQL, target, options, &block)
            log.detail "Output #{row_count} form records"
        end


        def extract_composite_form(target, filter = '%', options = {}, &block)
            log.info "Extracting composite form contents..."
            options[:bind_vars] = [filter]
            row_count = execute_query(FORM_PANES_SQL, target, options, &block)
            log.detail "Output #{row_count} composite form contents records"
        end


        def extract_form_layout(target, filter = '%', options = {}, &block)
            log.info "Extracting form layouts..."
            options[:bind_vars] = [filter]
            row_count = execute_query(FORM_LAYOUT_SQL, target, options, &block)
            log.detail "Output #{row_count} form layout records"
        end


        def extract_form_members(target, filter = '%', options = {}, &block)
            log.info "Extracting form members..."
            options[:bind_vars] = [filter]
            row_count = execute_query(FORM_MEMBERS_SQL, target, options, &block)
            log.detail "Output #{row_count} form member records"
        end


        def extract_form_calcs(target, filter = '%', options = {}, &block)
            log.info "Extracting form calculations..."
            options[:bind_vars] = [filter]
            row_count = execute_query(FORM_CALCS_SQL, target, options, &block)
            log.detail "Output #{row_count} form calc records"
        end


        def extract_form_menus(target, filter = '%', options = {}, &block)
            log.info "Extracting form menus..."
            options[:bind_vars] = [filter]
            row_count = execute_query(FORM_MENUS_SQL, target, options, &block)
            log.detail "Output #{row_count} form menu records"
        end


        def get_form_usage(filter = '%', options = {}, &block)
            options[:bind_vars] = [filter, filter]
            execute_query(FORM_USAGE_SQL, nil, options, &block)
        end


        def extract_smart_lists(target, options = {}, &block)
            log.info "Extracting smart lists..."
            default_options = {
                header_map: lambda{ |field| titleize(field.to_s) }
            }
            options = default_options.merge(options)
            row_count = execute_query(SMART_LISTS_SQL, target, options, &block)
            log.detail "Output #{row_count} smart list records"
        end


        def extract_smart_list_items(target, options = {}, &block)
            log.info "Extracting smart list items..."
            default_options = {
                header_map: lambda{ |field| titleize(field.to_s) }
            }
            options = default_options.merge(options)
            row_count = execute_query(SMART_LIST_ITEMS_SQL, target, options, &block)
            log.detail "Output #{row_count} smart list item records"
        end


        def extract_menu_items(target, options, &block)
            log.info "Extracting menu items..."
            row_count = execute_query(MENU_ITEMS_SQL, target, options, &block)
            log.detail "Output #{row_count} menu item records"
        end


        def extract_user_variables(target, options, &block)
            log.info "Extracting user variables..."
            row_count = execute_query(USER_VARIABLE_SQL, target, options, &block)
            log.detail "Output #{row_count} user variable records"
        end


        def extract_security(target, options, &block)
            log.info "Extracting security..."
            row_count = execute_query(SECURITY_ACCESS_SQL, target, options, &block)
            log.detail "Output #{row_count} security records"
        end


        private

        def execute_query(sql, target, options, &block)
            if target.nil?
                row_count = @conn.fetch(sql, *options[:bind_vars], &block).count
            elsif target.is_a?(Array)
                old_count = target.size
                row_count = @conn.select_to_array(sql, target, options, &block).count - old_count
                row_count -= 1 if options[:include_col_headers]
            elsif target.is_a?(String)
                row_count = select_to_file(sql, target, options, &block)
            else
                row_count = select_to_excel(sql, target, options, &block)
            end
            row_count
        end


        # Runs the specified query, and writes the output to the specified file
        def select_to_file(sql, file_name, options = {})
            bind_vars = options.fetch(:bind_vars, [])
            field_sep = options.fetch(:field_sep, "\t")
            append = options.fetch(:append, false)
            include_col_headers = options.fetch(:include_col_headers, true) && !append
            null_val = options.fetch(:null_val, '')
            decimals = options[:decimals] && "%.#{options[:decimals]}f"
            strip_line_breaks = options.fetch(:strip_line_breaks, false)
            quote_ambiguous_strings = options.fetch(:quote_ambiguous_strings, true)
            set_headers = options[:set_headers]
            header_map = options.fetch(:header_map, lambda { |field| field.to_s.upcase })
            file_encoding = options.fetch(:encoding, nil)


            if file_encoding
                file = File.open(file_name, append ? "a:#{file_encoding}" : "w:#{file_encoding}")
            else
                file = File.open(file_name, append ? 'a' : 'w')
            end
            begin
                ds = @conn.fetch(sql, *bind_vars)
                if include_col_headers
                    fields = ds.columns
                    headers = set_headers ? set_headers.call(fields) : fields
                    file.puts(headers.map(&header_map).join(field_sep))
                end

                row_count = 0
                ds.each do |row|
                    row = row.values
                    yield row if block_given?
                    fields = row.map do |cell|
                        case cell
                        when String
                            val = strip_line_breaks ? cell.gsub(/\r\n|[\n\r]/, ' ') : cell
                            if quote_ambiguous_strings && (val =~ /[,"\n\r]/ || val =~ /^\d+$/)
                                val = %Q{"#{val.gsub(/"/, '""')}"}
                            end
                            #STDERR.puts val if val =~ /[^\x00-\x7F]/
                            val
                        when Fixnum, BigDecimal, Float
                            if decimals
                                decimals % cell
                            else
                                cell
                            end
                        when nil then null_val
                        else cell
                        end
                    end
                    file.puts(fields.join(field_sep))
                    row_count += 1
                end
            ensure
                file.close
            end
            row_count
        end


        def titleize(string)
            string.gsub(/_/, ' ').gsub(/^\w/){ $&.upcase }.gsub(/\b('?[a-z])/){ $1.capitalize }
        end

    end

end
