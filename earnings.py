from datetime import datetime, timedelta
import re
import json

with open('earnings_dict.json', 'r') as f:
    date_to_company = json.load(f)

with open('company_url_dict.json', 'r') as f:
    company_to_url = json.load(f)

date_to_company = {datetime.strptime(date, '%Y-%m-%d'): companies for date, companies in date_to_company.items()}


def list_to_sentence(lst):
    if len(lst) == 1:
        return f"_{lst[0]}_"
    elif len(lst) == 2:
        return f"_{lst[0]}_ and _{lst[1]}_"
    else:
        return ', '.join([f"_{company}_" for company in lst[:-1]]) + f", and _{lst[-1]}_"

async def earnings_command(ctx, *args):
    valid_formats = [
        'YYYY-MM-DD',
        'MM/DD/YYYY',
        'YYYY/MM/DD',
        'YYYYMM (for a whole month)',
    ]
    if args:
        date_str = args[0]
        # YYYY-MM-DD format
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            query_date = datetime.strptime(date_str, '%Y-%m-%d')
        # MM/DD/YYYY format
        elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
            query_date = datetime.strptime(date_str, '%m/%d/%Y')
        # YYYY/MM/DD format
        elif re.match(r'\d{4}/\d{2}/\d{2}', date_str):
            query_date = datetime.strptime(date_str, '%Y/%m/%d')
        # YYYYMM format for a whole month
        elif re.match(r'\d{4}\d{2}', date_str):
            month_companies = {date: company for date, company in date_to_company.items() if date.strftime('%Y%m') == date_str}
            if month_companies:
                companies = ', '.join(month_companies.values())
                await ctx.send(f"{date.strftime('%Y-%m-%d')}: {companies}")
                return
            else:
                await ctx.send('No earnings in this month.')
                return
        else:
            await ctx.send(f'Invalid date format. Valid formats are: {", ".join(valid_formats)}.')
            return

        earnings_info = date_to_company.get(query_date, 'No earnings on this date.')
        await ctx.send(earnings_info)

    else:
        # No argument, show next week's companies including today
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Set the time to 00:00:00
        next_week = today + timedelta(days=7)
        next_week_companies = {date: company for date, company in date_to_company.items() if today <= date <= next_week}

        if next_week_companies:
            sentences = []
            for date, companies in next_week_companies.items():
                if isinstance(companies, list):
                    companies_str = list_to_sentence(companies)
                else:
                    companies_str = f"**{companies}**"
                    
                sentences.append(f"{date.strftime('%A, %Y-%m-%d')}: {companies_str}")

            final_output = 'Here are the gaming companies with earnings for the next 7 days:\n' + '\n'.join(sentences)
            await ctx.send(final_output)
        else:
            await ctx.send('No earnings in the next 7 days.')
