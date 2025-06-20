import requests
import os
import json
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

# ========== CONFIGURATION ==========

DISCOURSE_BASE_URL = "https://discourse.onlinedegree.iitm.ac.in/"
CATEGORY_SLUG = "courses/tds-kb"
CATEGORY_ID = 34

# Date range for filtering POSTS (what you actually want)
POST_START_DATE = "2025-01-01"  # Inclusive - posts must be after this date
POST_END_DATE = "2025-04-15"    # Inclusive - posts must be before this date

# Extended date range for fetching TOPICS (to catch older topics with recent activity)
TOPIC_FETCH_START_DATE = "2020-01-01"  # Go back much further to catch older topics
TOPIC_FETCH_END_DATE = "2025-04-15"    # Same as POST_END_DATE


RAW_COOKIE_STRING = "_forum_session=PsRhrEwAr994SGsUvB%2F6C4tIq76hTZQTWZbO0nL1Dagy6VB%2FONwObpvEwkSu71eiit4pR3RQygTxZeI1j9IPg686s6PmvqBsUA8JjuUgKNfFTUqqvGxTqPxkEUxaSXkXzFAtaSVLB03KCiWZboBhAUhqGDDIWWdUsd5CO0Co5w2ZTRV864rSrf5bh2NzbO%2FztVkVX8PgomK3wG5VBKS4yXamRIxSofnga%2BEqdtSLHPiXlM6i%2FWxkY8%2B4wdEsP0K5dRrtD6yg%2BCBmg5Cas%2Bo1QluuLs7hJDm1cVGU8BQHbwng5oWMUERo1Jvpeo8A55wn2F%2F9Ey3Fx9y89c0unzBr6gx4BhghQnYk4rVZniVOIJlDspNwEzBhcQrZ%2BTR8Lg%3D%3D--dFj8EQ%2FcK7UcWAGB--mumyiNOCYC%2BjIhUXT2CYXg%3D%3D; _t=ENuNKWtcMBkKs48Q80kCZRZttVXd0YIrEVBwrVse5LaMPmNUqoEl0SLJ4gHj%2BM2TtFnzSsI%2FqIp6WhlFC4DXMatILDH719qOL7Vc%2BWwXkiGsM90l65ps1MD%2B7%2FpjzfWxkXlSnFX244m55rogCxRH9iWPuv9vbRTuNvh7KjmIDs11wxqfCp3V8p1KJWaiFs4rgf5bfsMVe9oSeqkmSv4FUda6sNAiU9Fo23cgpdQWTXvcKGMTdqg7uZxZ7k9QnNuIt%2Fc1aRsDLoskcZwRTVCWerh6a1xkAFC5eMfpn27YGYNoNPzOCOVpZUg1VIirudYtN11v2g%3D%3D--gFiRTEcNKoVtChnB--mAyXPa7W2UadShaT%2BFNF2w%3D%3D"

OUTPUT_DIR = "discourse_json"
POST_ID_BATCH_SIZE = 50
MAX_CONSECUTIVE_PAGES_WITHOUT_NEW_TOPICS = 5

# ====================================

def parse_cookie_string(raw_cookie_string):
    """Parses a raw cookie string into a dictionary."""
    cookies = {}
    if not raw_cookie_string.strip():
        print("Warning: RAW_COOKIE_STRING is empty. Requests might fail if authentication is needed.")
        return cookies
    for cookie_part in raw_cookie_string.strip().split(";"):
        if "=" in cookie_part:
            key, value = cookie_part.strip().split("=", 1)
            cookies[key] = value
    return cookies


def get_topic_ids(base_url, category_slug, category_id, start_date_str, end_date_str, cookies):
    """Fetches topic IDs from a specific category within a date range."""
    url = urljoin(base_url, f"c/{category_slug}/{category_id}.json")
    topic_ids = []
    page = 0

    start_dt_naive = datetime.fromisoformat(start_date_str + "T00:00:00")
    start_dt = start_dt_naive.replace(tzinfo=timezone.utc)
    end_dt_naive = datetime.fromisoformat(end_date_str + "T23:59:59.999999")
    end_dt = end_dt_naive.replace(tzinfo=timezone.utc)

    print(f"Fetching topic IDs from category between {start_dt} and {end_dt}...")

    consecutive_pages_with_no_new_unique_topics = 0
    last_known_unique_topic_count = 0

    while True:
        paginated_url = f"{url}?page={page}"
        try:
            response = requests.get(paginated_url, cookies=cookies, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch page {page}: {e}")
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from page {page}. Content: {response.text[:200]}...")
            break

        topics_on_page = data.get("topic_list", {}).get("topics", [])

        if not topics_on_page:
            print(f"No more topics found on page {page} (API returned empty list).")
            break

        count_before_processing_page = len(set(topic_ids))

        for topic in topics_on_page:
            # Check both creation date and last activity date
            created_at_str = topic.get("created_at")
            last_posted_at_str = topic.get("last_posted_at")
            
            topic_should_be_included = False
            
            # Check if topic was created within the date range
            if created_at_str:
                try:
                    created_date = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                    if start_dt <= created_date <= end_dt:
                        topic_should_be_included = True
                except ValueError:
                    print(f"Warning: Could not parse created_at date '{created_at_str}' for topic ID {topic.get('id')}")
            
            # Check if topic has recent activity within the date range
            if not topic_should_be_included and last_posted_at_str:
                try:
                    last_posted_date = datetime.fromisoformat(last_posted_at_str.replace("Z", "+00:00"))
                    if start_dt <= last_posted_date <= end_dt:
                        topic_should_be_included = True
                        print(f"Including topic {topic.get('id')} due to recent activity (last post: {last_posted_date})")
                except ValueError:
                    print(f"Warning: Could not parse last_posted_at date '{last_posted_at_str}' for topic ID {topic.get('id')}")
            
            if topic_should_be_included:
                topic_ids.append(topic["id"])

        current_unique_topic_count = len(set(topic_ids))

        if current_unique_topic_count == last_known_unique_topic_count and topics_on_page:
            consecutive_pages_with_no_new_unique_topics += 1
            print(f"Page {page} did not yield any new unique topics. Consecutive stale pages: {consecutive_pages_with_no_new_unique_topics}.")
        else:
            consecutive_pages_with_no_new_unique_topics = 0

        last_known_unique_topic_count = current_unique_topic_count

        if consecutive_pages_with_no_new_unique_topics >= MAX_CONSECUTIVE_PAGES_WITHOUT_NEW_TOPICS:
            print(f"No new unique topics found for {MAX_CONSECUTIVE_PAGES_WITHOUT_NEW_TOPICS} consecutive pages. Assuming end of relevant category listing.")
            break

        more_topics_url = data.get("topic_list", {}).get("more_topics_url")
        if not more_topics_url:
            print(f"No 'more_topics_url' indicated on page {page}. Assuming this is the last page of topics.")
            break
        
        print(f"Fetched page {page}, {len(topics_on_page)} topics on page. Total unique topics found so far: {current_unique_topic_count}. Continuing...")
        page += 1

    final_unique_topic_ids = list(set(topic_ids))
    print(f"Total unique topics found in timeframe: {len(final_unique_topic_ids)}")
    return final_unique_topic_ids


def has_posts_in_date_range(topic_data, start_date_str, end_date_str):
    """Check if a topic has any posts within the specified date range."""
    start_dt_naive = datetime.fromisoformat(start_date_str + "T00:00:00")
    start_dt = start_dt_naive.replace(tzinfo=timezone.utc)
    end_dt_naive = datetime.fromisoformat(end_date_str + "T23:59:59.999999")
    end_dt = end_dt_naive.replace(tzinfo=timezone.utc)
    
    post_stream = topic_data.get("post_stream", {})
    posts = post_stream.get("posts", [])
    
    for post in posts:
        created_at_str = post.get("created_at")
        if created_at_str:
            try:
                post_date = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                if start_dt <= post_date <= end_dt:
                    return True
            except ValueError:
                continue
    
    return False


def filter_posts_by_date_range(topic_data, start_date_str, end_date_str):
    """Filter posts in topic_data to only include those within the date range."""
    start_dt_naive = datetime.fromisoformat(start_date_str + "T00:00:00")
    start_dt = start_dt_naive.replace(tzinfo=timezone.utc)
    end_dt_naive = datetime.fromisoformat(end_date_str + "T23:59:59.999999")
    end_dt = end_dt_naive.replace(tzinfo=timezone.utc)
    
    post_stream = topic_data.get("post_stream", {})
    posts = post_stream.get("posts", [])
    
    filtered_posts = []
    filtered_post_ids = set()
    
    for post in posts:
        created_at_str = post.get("created_at")
        if created_at_str:
            try:
                post_date = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                if start_dt <= post_date <= end_dt:
                    filtered_posts.append(post)
                    filtered_post_ids.add(post["id"])
            except ValueError:
                continue
    
    # Update the topic data with filtered posts
    topic_data["post_stream"]["posts"] = filtered_posts
    
    # Also update the stream to only include filtered post IDs
    original_stream = post_stream.get("stream", [])
    filtered_stream = [post_id for post_id in original_stream if post_id in filtered_post_ids]
    topic_data["post_stream"]["stream"] = filtered_stream
    
    return topic_data, len(filtered_posts)


def get_full_topic_json(base_url, topic_id, cookies):
    """Fetches the full topic JSON, including all posts by handling pagination."""
    initial_topic_url = urljoin(base_url, f"t/{topic_id}.json")
    print(f"Fetching initial data for topic {topic_id}")

    try:
        response = requests.get(initial_topic_url, cookies=cookies, timeout=30)
        
        # Add detailed error logging
        if response.status_code == 403:
            print(f"✗ Topic {topic_id}: Access forbidden (403) - topic may be private or restricted")
            return None
        elif response.status_code == 404:
            print(f"✗ Topic {topic_id}: Not found (404) - topic may have been deleted")
            return None
        elif response.status_code == 429:
            print(f"✗ Topic {topic_id}: Rate limited (429) - too many requests")
            return None
        
        response.raise_for_status()
        topic_data = response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Topic {topic_id}: Network error - {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"✗ Topic {topic_id}: JSON decode error - {e}")
        print(f"Response content: {response.text[:200]}...")
        return None

    post_stream = topic_data.get("post_stream")
    if not post_stream or "stream" not in post_stream or "posts" not in post_stream:
        print(f"✗ Topic {topic_id}: Invalid post_stream structure")
        return None

    all_post_ids_in_stream = post_stream.get("stream", [])
    loaded_post_ids = {post["id"] for post in post_stream.get("posts", [])}

    all_post_ids_in_stream = [pid for pid in all_post_ids_in_stream if pid is not None]
    missing_post_ids = [pid for pid in all_post_ids_in_stream if pid not in loaded_post_ids]

    print(f"Topic {topic_id}: Total posts in stream: {len(all_post_ids_in_stream)}, Initially loaded: {len(loaded_post_ids)}, Missing: {len(missing_post_ids)}")

    if missing_post_ids:
        fetched_additional_posts = []
        for i in range(0, len(missing_post_ids), POST_ID_BATCH_SIZE):
            batch_ids = missing_post_ids[i:i + POST_ID_BATCH_SIZE]
            query_params = [("post_ids[]", pid) for pid in batch_ids]
            posts_url = urljoin(base_url, f"t/{topic_id}/posts.json")

            print(f"Fetching batch of {len(batch_ids)} posts for topic {topic_id}")

            try:
                batch_response = requests.get(posts_url, params=query_params, cookies=cookies, timeout=60)
                batch_response.raise_for_status()
                batch_data = batch_response.json()

                if isinstance(batch_data, list):
                    fetched_additional_posts.extend(batch_data)
                elif "post_stream" in batch_data and "posts" in batch_data["post_stream"]:
                    fetched_additional_posts.extend(batch_data["post_stream"]["posts"])
                elif "posts" in batch_data and isinstance(batch_data["posts"], list):
                    fetched_additional_posts.extend(batch_data["posts"])
                else:
                    print(f"Warning: Unexpected JSON structure for post batch in topic {topic_id}.")

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch post batch for topic {topic_id}: {e}")
            except json.JSONDecodeError:
                print(f"Failed to decode JSON for post batch in topic {topic_id}.")

        if fetched_additional_posts:
            existing_posts_in_topic_data = {post['id']: post for post in topic_data["post_stream"]["posts"]}
            for post in fetched_additional_posts:
                if post['id'] not in existing_posts_in_topic_data:
                    topic_data["post_stream"]["posts"].append(post)
                    existing_posts_in_topic_data[post['id']] = post

            post_id_to_post_map = {post['id']: post for post in topic_data["post_stream"]["posts"]}
            sorted_posts = []
            for post_id_val in all_post_ids_in_stream:
                if post_id_val in post_id_to_post_map:
                    sorted_posts.append(post_id_to_post_map[post_id_val])

            topic_data["post_stream"]["posts"] = sorted_posts

    return topic_data


def save_topic_json(topic_id, json_data, output_dir):
    """Saves the topic JSON data to a file."""
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"topic_{topic_id}.json")
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"Error saving topic {topic_id} to {filepath}: {e}")


def test_single_topic(topic_id):
    """Test downloading a single topic for debugging."""
    cookies = parse_cookie_string(RAW_COOKIE_STRING)
    topic_data = get_full_topic_json(DISCOURSE_BASE_URL, topic_id, cookies)
    
    if topic_data:
        print(f"✓ Successfully fetched topic {topic_id}")
        if has_posts_in_date_range(topic_data, POST_START_DATE, POST_END_DATE):
            print(f"✓ Topic {topic_id} has posts in date range")
        else:
            print(f"✗ Topic {topic_id} has no posts in date range")
    else:
        print(f"✗ Failed to fetch topic {topic_id}")


def main():
    """Main function to orchestrate the downloading process."""
    print("Script started.")
    print(f"Topic fetch date range: {TOPIC_FETCH_START_DATE} to {TOPIC_FETCH_END_DATE}")
    print(f"Post filter date range: {POST_START_DATE} to {POST_END_DATE}")
    
    cookies = parse_cookie_string(RAW_COOKIE_STRING)
    if not cookies and DISCOURSE_BASE_URL != "https://meta.discourse.org/":
        print("Warning: Running without cookies. This may fail for private forums or specific content.")

    # Fetch topics using the extended date range
    topic_ids = get_topic_ids(
        DISCOURSE_BASE_URL,
        CATEGORY_SLUG,
        CATEGORY_ID,
        TOPIC_FETCH_START_DATE,  # Extended range to catch older topics
        TOPIC_FETCH_END_DATE,
        cookies
    )

    if not topic_ids:
        print("No topic IDs found for the given criteria. Exiting.")
        return

    total_topics = len(topic_ids)
    success_downloads = 0
    failed_topic_ids = []
    topics_with_relevant_posts = 0

    print(f"\nStarting download of {total_topics} topics...\n")

    for i, topic_id in enumerate(topic_ids, 1):
        print(f"--- [{i}/{total_topics}] Processing topic ID: {topic_id} ---")
        
        # Add delay to prevent rate limiting
        if i > 1:  # Don't delay on first request
            time.sleep(1)  # 1 second delay between requests
        
        topic_json_data = get_full_topic_json(DISCOURSE_BASE_URL, topic_id, cookies)
        
        if topic_json_data:
            # Check if this topic has posts within our target date range
            if has_posts_in_date_range(topic_json_data, POST_START_DATE, POST_END_DATE):
                # Filter posts to only include those within our date range
                filtered_data, post_count = filter_posts_by_date_range(
                    topic_json_data, POST_START_DATE, POST_END_DATE
                )
                
                if post_count > 0:
                    save_topic_json(topic_id, filtered_data, OUTPUT_DIR)
                    success_downloads += 1
                    topics_with_relevant_posts += 1
                    print(f"✓ Topic {topic_id} saved with {post_count} relevant posts")
                else:
                    print(f"✗ Topic {topic_id} has no posts in target date range after filtering")
            else:
                print(f"✗ Topic {topic_id} has no posts within target date range - skipping")
        else:
            print(f"✗ Failed to get complete data for topic {topic_id}")
            failed_topic_ids.append(topic_id)

    print("\n========= SUMMARY =========")
    print(f"Total topics fetched from category: {total_topics}")
    print(f"Topics with posts in target date range: {topics_with_relevant_posts}")
    print(f"Successfully downloaded and filtered: {success_downloads} topics")
    print(f"Failed to download/process: {len(failed_topic_ids)} topics")
    if failed_topic_ids:
        print("Failed topic IDs:", failed_topic_ids)
    print(f"Downloaded files are in: {os.path.abspath(OUTPUT_DIR)}")
    print("Script finished.")


if __name__ == "__main__":
    main()

