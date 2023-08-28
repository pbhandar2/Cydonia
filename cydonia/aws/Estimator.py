""" This class estimates the cost of serverless application in AWS. """

class Estimator:
    def __init__(self, active_user_count, passive_user_count, **kwargs):
        self.active_user_count = active_user_count 
        self.passive_user_count = passive_user_count 

        ## AWS: lambda cost 
        self.lambda_req_cost = 0.2/1e6
        self.lambda_memory_cost_per_ms = {
            128: 0.0000000021,
            512: 0.0000000083
        }

        ## AWS: S3 cost 
        self.data_transfer_out_s3_cost_per_gb = {
            10000: 0.09,
            40000: 0.085,
            100000: 0.07,
            150000: 0.05 
        }
        self.s3_get_req_cost = 0.04/1e3

        ## AWS: Cognito (User auth) cost
        self.sms_verify_cost = 0.01 
        self.auth_cost = {
            50000: 0.05,
            100000: 0.035,
            190000: 0.02,
            9900000: 0.015,
            10000000: 0.01
        }

        ## AWS: Cloudfront cost 
        self.data_transfer_out_s3_cost_per_gb = {
            10000: 0.085,
            50000: 0.08,
            150000: 0.06,
            500000: 0.04,
            1024000: 0.03
        }

        ## AWS: API gateway cost 
        self.api_gateway_cost = {
            3000000: 1/3000000,
            6000000: 0.95/6000000
        }

        ## AWS: Dynamodb cost 
        self.storage_cost_per_gb = 0.5
        self.read_unit_cost = 0.25/1e6
        self.write_unit_cost = 1.25/1e6

        
        # ASSUMPTIONS
        ## lambda
        ### lambda for timeline access 
        self.tl_lambda_runtime_ms = 20 
        self.tl_lambda_memory_mb = 128 
        self.tl_total_lambda_cost = self.lambda_req_cost + \
                                        (self.tl_lambda_runtime_ms * \
                                            self.lambda_memory_cost_per_ms[self.tl_lambda_memory_mb])
        
        ### lambda to create pollen 
        self.new_pollen_lambda_runtime_ms = 5 
        self.new_pollen_lambda_memory_mb = 128
        self.new_pollen_total_lambda_cost = self.lambda_req_cost + \
                                                (self.new_pollen_lambda_runtime_ms * \
                                                    self.lambda_memory_cost_per_ms[self.new_pollen_lambda_memory_mb])

        ### lambda to vote 
        self.vote_lambda_runtime_ms = 5 
        self.vote_lambda_memory_mb = 128
        self.vote_total_lambda_cost = self.lambda_req_cost + \
                                        (self.vote_lambda_runtime_ms * \
                                            self.lambda_memory_cost_per_ms[self.vote_lambda_memory_mb])

        # lambda to signup
        self.signup_lambda_runtime_ms = 5 
        self.signup_lambda_memory_mb = 128
        self.signup_total_lambda_cost = self.lambda_req_cost + \
                                        (self.signup_lambda_runtime_ms * \
                                            self.lambda_memory_cost_per_ms[self.signup_lambda_memory_mb])

        ### lambda to access someones profile
        self.profile_lambda_runtime_ms = 20
        self.profile_lambda_memory_mb = 128
        self.profile_total_lambda_cost = self.lambda_req_cost + \
                                            (self.profile_lambda_runtime_ms * \
                                                self.lambda_memory_cost_per_ms[self.profile_lambda_memory_mb])

        ### lambda to upgrade user cred
        self.upgarde_cred_lambda_runtime_ms = 5
        self.upgarde_cred_lambda_memory_mb = 128
        self.upgarde_cred_total_lambda_cost = self.lambda_req_cost + \
                                                (self.upgarde_cred_lambda_runtime_ms * \
                                                    self.lambda_memory_cost_per_ms[self.upgarde_cred_lambda_memory_mb])

        ### lambda to follow user 
        self.follow_lambda_runtime_ms = 5
        self.follow_lambda_memory_mb = 128
        self.follow_total_lambda_cost = self.lambda_req_cost + \
                                            (self.follow_lambda_runtime_ms * \
                                                self.lambda_memory_cost_per_ms[self.follow_lambda_memory_mb])       

        ### lambda to search
        self.search_lambda_runtime_ms = 20
        self.search_lambda_memory_mb = 128
        self.search_total_lambda_cost = self.lambda_req_cost + \
                                            (self.search_lambda_runtime_ms * \
                                                self.lambda_memory_cost_per_ms[self.search_lambda_memory_mb])     
        
        ## S3 
        self.average_s3_asset_size_mb = 1 

        ## DYNAMO DB
        self.tl_read_unit = 2
        self.profile_read_unit = 2 
        self.new_pollen_write_unit = 1 
        self.vote_write_unit = 1 
        self.upgrade_cred_write_unit = 1
        self.follow_write_unit = 1 
        self.tl_generation_read_unit = 2 
        self.t1_generation_write_unit = 2 
        self.search_read_unit = 2
        
        ## USER BEHAVIOR 
        ### timeline access per user 
        """ How many times will the user access the timeline in a day? This causes
            the following billable actions:
                1. LAMBDA TRIGGER: Redirect to S3 for web assets and backend for data
                    i. 1k pollen and if we return 1024 of then we need 1MB per timeline call 
                    ii. prolly running for 20ms 
                2. S3 READ: Webpage assets (HTML/CSS): assuming 1MB 
                3. API ACCESS: API get timeline access
                4. DYNAMO READ: 1 read unit for timeline 
                5. CLOUDFRONT OUT: Outgoing data of size 2MB """
        self.tl_access_per_active_user = 5
        if 'tl_access_per_active_user' in kwargs:
            self.tl_access_per_active_user = kwargs['tl_access_per_active_user']

        self.tl_access_per_passive_user = 1
        if 'tl_access_per_passive_user' in kwargs:
            self.tl_access_per_passive_user = kwargs['tl_access_per_passive_user']

        ### question creation per user 
        """ How many questions will a user create in a day? This causes
            the following billable actions:
                1. LAMBDA TRIGGER: Direct to POST API call 
                2. CLOUDFRONT IN: Incoming data of 1MB 
                3. API Access: API call to POST the pollen 
                4. DYNAMO WRITE: 1 write unit for POST pollen 
                5. CLOUDFRONT OUT: Outgoing data of 1MB """
        self.q_per_active_user = 10 
        if 'q_per_active_user' in kwargs:
            self.q_per_active_user = kwargs['q_per_active_user']

        self.q_per_passive_user = 1 
        if 'q_per_passive_user' in kwargs:
            self.q_per_passive_user = kwargs['q_per_passive_user']

        ### vote per user 
        """ How many votes will a user submit in a day? This causes
            the following billable actions:
                1. LAMBDA TRIGGER: Direct to POST API call to update
                2. CLOUDFRONT IN: Incoming data of 1KB
                3. API Access: API call to POST the pollen 
                4. DYNAMO WRITE: 1 write unit for POST pollen 
                5. CLOUDFRONT OUT: Outgoing data of 1MB """
        self.votes_per_active_user = 50
        if 'votes_per_active_user' in kwargs:
            self.votes_per_active_user = kwargs['votes_per_active_user']

        self.votes_per_passive_user = 5
        if 'votes_per_passive_user' in kwargs:
            self.votes_per_passive_user = kwargs['votes_per_passive_user']
        
        ### profile page visit 
        """ On average how often will profile pages of a user be accessed?
        """
        self.profile_page_access_per_active_user = 5 
        if 'profile_page_access_per_active_user' in kwargs:
            self.profile_page_access_per_active_user = kwargs['profile_page_access_per_active_user']

        self.profile_page_access_per_passive_user = 5 
        if 'profile_page_access_per_passive_user' in kwargs:
            self.profile_page_access_per_passive_user = kwargs['profile_page_access_per_passive_user']

        ### profile page visit 
        """ On average how often will users search? 
        """
        self.search_per_active_user = 20 
        if 'search_per_active_user' in kwargs:
            self.search_per_active_user = kwargs['search_per_active_user']

        self.search_per_passive_user = 5 
        if 'profile_page_access_per_passive_user' in kwargs:
            self.search_per_passive_user = kwargs['search_per_passive_user']

        ### follow behavior
        self.new_signup_follow_count = 10 
        if 'new_signup_follow_count' in kwargs:
            self.new_signup_follow_count = kwargs['new_signup_follow_count']
        
        self.old_user_follow_rate_per_month = 5
        if 'old_user_follow_rate_per_month' in kwargs:
            self.old_user_follow_rate_per_month = kwargs['old_user_follow_rate_per_month']

        ## Size of pollen in bytes
        self.pollen_size_byte = 1024 
        if 'pollen_size_byte' in kwargs:
            self.pollen_size_byte = kwargs['pollen_size_byte']

        ## Number of pollen per timeline 
        self.num_pollen_per_timeline = 1024
        if 'num_pollen_per_timeline' in kwargs:
            self.num_pollen_per_timeline = kwargs['num_pollen_per_timeline']
        
        self.timeline_size_byte = self.num_pollen_per_timeline * self.pollen_size_byte
        self.tl_outgoing_data = 1024
        


    def get_asset_request_mb(self, active_user_count, passive_user_count):
        pass
    

    def get_monthly_data_out_cloudfront(self):
        pass 
        
    

    def compute_cost(self, price_dict, count):
        cost = 0.0 
        count_tracked = 0.0 
        sorted_keys = sorted(price_dict.keys())
        for key in sorted_keys:
            cur_count = key - count_tracked
            if key <= count:
                cost += (cur_count * price_dict[key])
            else:
                diff = count - count_tracked
                cost += (diff * price_dict[key])
                break 
            count_tracked = key
        else:
            max_key = max(sorted_keys)
            extra_count = count - max_key
            if extra_count > 0:
                cost += (extra_count * price_dict[max_key])
        return cost 
        

    def get_monthly_cost(self):
        cost_array = []
        storage_size_gb = 0.0 
        prev_total_user_count = 0.0 
        auth_cost_list = []
        for month_index in range(len(self.active_user_count)):
            print("Computing cost for month {} with {} active and {} passive users".format(month_index, self.active_user_count[month_index],  self.passive_user_count[month_index] ))
            monthly_cost = 0.0
            outgoing_data_byte = 0.0 
            api_access_count = 0 
            dynamo_read_unit = 0 
            dynamo_write_unit = 0 
            lambda_access_count = 0 

            # user-onboarding cost 
            total_user_count = self.active_user_count[month_index] + self.passive_user_count[month_index]
            user_onboarded = total_user_count - prev_total_user_count
            user_onboarded_cost = user_onboarded * self.sms_verify_cost
            print("Onboarding cost is {}\n".format(user_onboarded_cost))

            monthly_cost += (user_onboarded_cost + (user_onboarded) * self.signup_total_lambda_cost)

            print("Signup lambda cost is {}\n".format((user_onboarded) * self.signup_total_lambda_cost))

            # user auth cost 
            auth_cost = self.compute_cost(self.auth_cost, total_user_count)
            monthly_cost += auth_cost
            print("Auth cost is {}\n".format(auth_cost))

            

            lambda_access_count += user_onboarded

            # timeline cost 
            total_tl_access = (self.tl_access_per_active_user * self.active_user_count[month_index]) + \
                                (self.tl_access_per_passive_user * self.passive_user_count[month_index])
            monthly_cost += (total_tl_access * self.tl_total_lambda_cost)
            print("TL lambda cost is {}\n".format(total_tl_access * self.tl_total_lambda_cost))

            outgoing_data_byte += (total_tl_access * self.tl_outgoing_data)
            api_access_count += total_tl_access 
            dynamo_read_unit += (total_tl_access * self.tl_read_unit)

            # vote cost 
            total_votes = (self.votes_per_active_user * self.active_user_count[month_index]) + \
                                (self.votes_per_passive_user * self.passive_user_count[month_index])
            monthly_cost += (total_votes * self.vote_total_lambda_cost)
            print("Vote lambda cost is {}\n".format(total_votes * self.vote_total_lambda_cost))

            api_access_count += total_votes
            dynamo_write_unit += self.vote_write_unit

            # create cost 
            total_q = (self.q_per_active_user * self.active_user_count[month_index]) + \
                                (self.q_per_passive_user * self.passive_user_count[month_index])
            monthly_cost += (total_q * self.new_pollen_total_lambda_cost)

            print("Create lambda cost is {}\n".format(total_q * self.new_pollen_total_lambda_cost))

            api_access_count += total_q
            dynamo_write_unit += self.vote_write_unit

            # profile cost 
            profile_access_count = (self.profile_page_access_per_active_user * self.active_user_count[month_index]) + \
                                        (self.profile_page_access_per_passive_user * self.passive_user_count[month_index])
            monthly_cost += (profile_access_count * self.profile_total_lambda_cost)

            print("Profile lambda cost is {}\n".format(profile_access_count * self.profile_total_lambda_cost))

            outgoing_data_byte += (profile_access_count * self.tl_outgoing_data)
            api_access_count += profile_access_count
            dynamo_read_unit += self.profile_read_unit

            # follow count 
            follow_count = (prev_total_user_count * self.old_user_follow_rate_per_month) + (user_onboarded * self.new_signup_follow_count)
            monthly_cost += (follow_count * self.follow_total_lambda_cost)

            print("Follow lambda cost is {}\n".format(follow_count * self.follow_total_lambda_cost))

            api_access_count += follow_count
            dynamo_write_unit += self.follow_write_unit

            # search 
            search_count = (self.search_per_active_user * self.active_user_count[month_index]) + \
                                (self.search_per_passive_user * self.passive_user_count[month_index])
            monthly_cost += (search_count * self.search_total_lambda_cost)

            print("Search lambda cost is {}\n".format(search_count * self.search_total_lambda_cost))

            outgoing_data_byte += (profile_access_count * self.tl_outgoing_data)
            api_access_count += search_count
            dynamo_read_unit += self.search_read_unit

            print("Total lambda cost is {}\n".format(monthly_cost))

            # all lambda cost is incorporated by now 
            # S3 cost 
            total_link_access = total_tl_access + profile_access_count
            total_s3_asset_gb = (total_link_access * self.average_s3_asset_size_mb)/1e3
            s3_data_transfer_cost = self.compute_cost(self.data_transfer_out_s3_cost_per_gb, total_s3_asset_gb)
            s3_get_req_cost = self.s3_get_req_cost * total_link_access
            monthly_cost += (s3_data_transfer_cost + s3_get_req_cost)

            print("Total S3 cost is: {}\n".format(s3_data_transfer_cost + s3_get_req_cost))

            # api access cost 
            monthly_cost += self.compute_cost(self.api_gateway_cost, api_access_count)
            print("API cost is: {}\n".format(self.compute_cost(self.api_gateway_cost, api_access_count)))

            # cloudfront cost 
            monthly_cost += self.compute_cost(self.data_transfer_out_s3_cost_per_gb, outgoing_data_byte/1e9)
            print("Cloudfront cost is: {}\n".format(self.compute_cost(self.data_transfer_out_s3_cost_per_gb, outgoing_data_byte/1e9)))

            # db storage cost 
            storage_cost = storage_size_gb * self.storage_cost_per_gb
            io_cost = (dynamo_read_unit * self.read_unit_cost) + (dynamo_write_unit * self.write_unit_cost)
            monthly_cost += (storage_cost + io_cost)

            print("DB cost is: {}\n".format(storage_cost + io_cost))

            print("Total cost for month {} with {} active and {} passive users is {}".format(month_index, self.active_user_count[month_index],  self.passive_user_count[month_index], monthly_cost))

            storage_size_gb += (total_q/1e3)
            prev_total_user_count = total_user_count
            cost_array.append(monthly_cost)
            auth_cost_list.append(auth_cost + (user_onboarded * self.signup_total_lambda_cost) + user_onboarded_cost)

        return cost_array, auth_cost_list

