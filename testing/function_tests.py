# from ..src.common_package.time_taken_tasks import determine_category



def determine_category(time_taken):
    
    
    time_ranges = [
        ('1min+', 100000, None),
        ('30s+', 30000, 99999),
        ('10s-30s', 10000, 30000),
        ('5s-10s', 5000, 10000),
        ('2s-5s', 2000, 5000),
        ('1s-2s', 1000, 2000),
        ('500ms-1s', 200, 999),
        ('200ms-500ms', 200, 499),
        ('100ms-200ms', 100, 199),
        ('<100m', 0, 99)
    ]
    
    for r in time_ranges:
        
        if r[2]:
            if time_taken >= r[1] and time_taken < r[2]:
                return r
        else:
            if time_taken >= r[1]:
                return r
    

# time_taken = 260528
time_taken = 260528
print(determine_category(time_taken))